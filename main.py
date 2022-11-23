import boto3
import json
from mambu_rest_api import get_config_file, get_dates_to_process,get_tables_to_process
from datetime import datetime,date, timedelta
import re
#CONNECTION TO AWS CLIENT
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id='AKIA3DB37LF4PEND6LCX',
    aws_secret_access_key='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')
#DEFINE BUCKET AND KEY
bucket = s3.Bucket("mambu-api-rd")
config_key = "files/config.json"
#GET CONFIG FILE
config_file_data = json.loads(get_config_file(bucket,config_key)) 
#print(config_file_data)
#DEFINE DAYS TO PROCESS BASED ON REPROCESSING VALUE
dates_to_process = get_dates_to_process(config_file_data)
#print(dates_to_process)
#GET TABLES TO PROCESS
tables_to_process = get_tables_to_process(config_file_data)
print(tables_to_process)
#SAVE FILE INTO AWS


