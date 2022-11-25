import boto3
import json
from mambu_rest_api import get_config_file, get_dates_to_process,get_tables_to_process,put_object_in_bucket
from datetime import datetime,date, timedelta
import re
#CONNECTION TO AWS CLIENT
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id='AKIA3DB37LF4OMETDK3G',
    aws_secret_access_key='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')
#DEFINE BUCKET AND KEY
bucket = s3.Bucket("mambu-api-rd")
bucket_name ="mambu-api-rd"
config_key = "files/config.json"
write_key = "files/"
#GET CONFIG FILE
config_file_data = json.loads(get_config_file(bucket,config_key)) 
#DEFINE DAYS TO PROCESS BASED ON REPROCESSING VALUE
dates_to_process = get_dates_to_process(config_file_data)
#GET TABLES TO PROCESS
tables_to_process = get_tables_to_process(config_file_data)
#WRITE FILE INTO AWS AND CREATE DIRECTORY FOLDERS FOR DATES PROCESSED
put_object_in_bucket(s3,bucket_name,write_key,dates_to_process)










