import boto3
import time
import collections
import datetime
    
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
months = ["11","12"]
dates = ["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]


database = "default" #Change this to relevant name
athena_result_bucket = "s3://aws-athena-query-results-{}-{}/".format(account_id, region1)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('athena_partition_detail')

response = table.scan()
   
present_partition = collections.defaultdict(tree)

for items in response['Items']:
    date = items['partition_date']
    region = items['region']
#    print(region,date)
    present_partition[region][items['partition_date']] = 1

def lambda_handler(event, context):

    for region in regions:
        for month in months:
            for date in dates :
                if not present_partition[region][str(year)+"-"+str(month)+"-"+str(date)]:
                    query = "ALTER TABLE cloudtrail_logs_p ADD PARTITION (region='"+region+"',year='2019',month='"+month+"',day='"+date+"')\
                    location 's3://<bucket>/AWSLogs/<account_id>/CloudTrail/" +region+ "/2019/" +month+ "/" +date+ "'"
                    print(query)
    
                    response = athena_client.start_query_execution(
                        QueryString=query,
                        QueryExecutionContext={
                            'Database': database
                        },
                        ResultConfiguration={
                            'OutputLocation': athena_result_bucket,
                        }
                    )
    
                    #query_execution_id = response["QueryExecutionId"]
                    #state = "QUEUED"
        
                    # while state == "QUEUED" or state == "RUNNING":
                    #     stateResult = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                    #     state = stateResult['QueryExecution']['Status']['State']
                    #     print(state)
                    #     time.sleep(2)
                    
                    table.put_item(
                        Item={        
                        'region': region,
                        'partition_date': str(year) +"-"+ str(month) +"-"+ str(date),
                        }
                    )
                
