import boto3
import time
import collections
from datetime import date

BOTO_SESSION = boto3.Session()
athena_client = boto3.client('athena')
ec2_client = boto3.client('ec2')
dynamodb_client = boto3.client('dynamodb')

global tree
def tree():
    return collections.defaultdict(tree)

account_id = BOTO_SESSION.client('sts').get_caller_identity().get('Account')
region1 = BOTO_SESSION.region_name

regions = []

year = 2019
response = ec2_client.describe_regions()
for region in response['Regions'] :
    regions.append(region['RegionName'])

print(regions)    

database = "default" #Change this to relevant name
athena_result_bucket = "s3://aws-athena-query-results-{}-{}/".format(account_id, region1)


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('athena_partition_detail')

response = table.scan()
   
present_partition = collections.defaultdict(tree)

for items in response['Items']:
    region = items['region']
    present_partition[region][items['partition_date']] = 1


def lambda_handler(event, context):
    today = date.today()
    currentDate = today.strftime("%Y-%m-%d")
    currentDate = str(currentDate)
    
    for region in regions:
        if not present_partition[region][currentDate]:
            year = currentDate[:4]
            month = currentDate[5:7]
            datetoday = currentDate[-2:]
            query = "ALTER TABLE cloudtrail_logs_p ADD PARTITION (region='"+region+"',year='2019',month='"+month+"',day='"+datetoday+"')\
                location 's3://<bucket>/AWSLogs/<account-id>/CloudTrail/" +region+ "/2019/" +month+ "/" +datetoday+ "'"
            
            #print(query)   
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                'Database': database
                },
                ResultConfiguration={
                    'OutputLocation': athena_result_bucket,
                }
            )
            
            query_execution_id = response["QueryExecutionId"]
            state = "QUEUED"
        
            while state == "QUEUED" or state == "RUNNING":
                stateResult = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                state = stateResult['QueryExecution']['Status']['State']
                print(state)
                time.sleep(2)
            
            table.put_item(
                Item={        
                    'region': region,
                    'partition_date': currentDate,
                }
            )
    
