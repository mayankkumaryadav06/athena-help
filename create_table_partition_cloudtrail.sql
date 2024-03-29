CREATE EXTERNAL TABLE cloudtrail_logs_p (

         eventversion STRING,
         userIdentity STRUCT< type:STRING,
         principalid:STRING,
         arn:STRING,
         accountid:STRING,
         invokedby:STRING,
         accesskeyid:STRING,
         userName:STRING,
         sessioncontext:STRUCT< attributes:STRUCT< mfaauthenticated:STRING,
         creationdate:STRING>,
         sessionIssuer:STRUCT< type:STRING,
         principalId:STRING,
         arn:STRING,
         accountId:STRING,
         userName:STRING>>>,
         eventTime STRING,
         eventSource STRING,
         eventName STRING,
         awsRegion STRING,
         sourceIpAddress STRING,
         userAgent STRING,
         errorCode STRING,
         errorMessage STRING,
         requestParameters STRING,
         responseElements STRING,
         additionalEventData STRING,
         requestId STRING,
         eventId STRING,
         resources ARRAY<STRUCT<ARN:STRING,
        accountId: STRING,
        type:STRING>>,
         eventType STRING,
         apiVersion STRING,
         readOnly STRING,
         recipientAccountId STRING,
         serviceEventDetails STRING,
         sharedEventID STRING,
         vpcEndpointId STRING 
		 
) PARTITIONED BY (
        region string,
         year string,
         month string,
         day string
) 
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde' 
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
LOCATION 's3://<cloudtrail_bucket_name>/AWSLogs/<accountID>/CloudTrail/'
