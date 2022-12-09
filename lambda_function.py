import boto3
import json
from mambu_rest_api import get_config_file, get_dates_to_process,get_tables_to_process,put_object_in_bucket, get_encoded_keys_params,get_headers, get_mappings, get_table #,get_api_encodedkey
from datetime import datetime,date, timedelta
import re
#import requests
import csv
import pandas as pd 


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    #DEFINE BUCKET AND KEY
    bucket_name ="mambu-api-rd"
    bucket = s3.Bucket("mambu-api-rd")
    config_key = "files/config.json"
    write_key = "files/"
    #GET CONFIG FILE
    config_file_data = json.loads(get_config_file(bucket,config_key)) 
    #DEFINE DAYS TO PROCESS BASED ON REPROCESSING VALUE
    dates_to_process = get_dates_to_process(config_file_data)
    #GET TABLES TO PROCESS
    tables_to_process = get_tables_to_process(config_file_data)
    #GET HEADERS
    headers = get_headers(config_file_data)
    #GET the API To fetch list of encoded keys
    #api_encodedkey= get_api_encodedkey(config_file_data)
    #GET LIST OF ENCODED KEYS AND PARAMETERS
    #list_of_encodedkeys=get_encoded_keys_params(api_encodedkey, headers)

    #GET APIs for the tables 
    Mapping= get_mappings(config_file_data)
    
    #API CALL FOR THE TABLES, SEND TO S3 BUCKET 
    get_table(tables_to_process,headers,Mapping,dates_to_process)
    
