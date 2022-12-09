import boto3
from datetime import datetime,date, timedelta
import re
import requests
import json
import csv
import pandas as pd


#function to retrive the JSON body
def get_config_file(bucket,config_key):
    for obj in bucket.objects.all(): #cycle all object until the desired object is found using combination of bucket and key
        key = obj.key
        if key == config_key:
            body = obj.get()['Body'].read().decode('utf-8') #save JSON body info
    return body

#function to retrieve dates to be processed
def get_dates_to_process(config_file_data):
    now = datetime.now() #define current date
    dates_to_process=[]  #initialize dates list
    if config_file_data['reprocessing'] == "false": #Check "reprocessing" 
        dates_to_process.append(now.strftime("%Y-%m-%d")) #If reprocessing = false then we just want to process the current date so we append the current date to the list
    else:
        day_to_process = config_file_data['dt_information'] #If reprocessing = true then we want to process all the dates between "dt_information" value until current date
        dates_to_process.append(day_to_process) #store "dt_information" value in the list
        while day_to_process != now.strftime("%Y-%m-%d"): #cicle dates until current date
            day_to_process = (datetime.strptime(day_to_process,"%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") #save new date in a new variable 
            dates_to_process.append(day_to_process) #append new date to the list
    return dates_to_process

#get the headers from config file 
def get_headers(config_file_data):
    return config_file_data['headers']
    
#get the api name to fetch the encoded key
#def get_api_encodedkey(config_file_data):
#    return config_file_data['api_encoded_key']
    
#Get the mappings of the tables and APIs
def get_mappings(config_file_data):
    return config_file_data['Mapping']    

#
#function to retrieve tables to be processed
def get_tables_to_process (config_file_data):
    return re.findall(r'\S+', config_file_data['tables'])

#function to upload files in AWS with correct directory based on dates processed
## desenvolver 2 formas de ficheiros, JSON bruto e JSON já flatten para modo estruturado 
def put_object_in_bucket (s3,bucket_name,write_key,dates_to_process):
    for date_process in dates_to_process:
        parsed_date = date_process.split('-')
        for parse in parsed_date:
            year = parsed_date[0]
            month = parsed_date[1]
            day = parsed_date[2]   
        s3.Object(bucket_name, write_key +year+'/'+month+'/'+day ).put(Body="TESTE")

#get encoded keys in order to have access data from different tables 
def get_encoded_keys_params(api, headers):
    URL = "http://deloittept.sandbox.mambu.com/api/"
    api = api
    offset = 0
    limit = 500
    list_of_encodedkeys = []
    while True:
        parameters = {
                "limit":limit,
                "offset":offset
            }  
        response = requests.get(URL + api,headers=headers,params = parameters)
        data = response.json()
        if(len(data) == 0):
            break
        else:
            for i in range(len(data)):
                 list_of_encodedkeys.append(data[i]["encodedKey"])
            offset += limit
    
    return list_of_encodedkeys
    
#API Call to fetch the intended tables 
def get_table(table_name,headers,Mapping,dates_to_process):
    URL = "https://deloittept.sandbox.mambu.com/api"
    for  date  in  dates_to_process:
        table_json =[]
        payload = json.dumps({
              "filterCriteria": [
                {
                  "field": "creationDate",
                  "operator": "ON",
                  "value": date
                }
              ]
            })

        for t in range(len(table_name)):
            table_df = pd.DataFrame()
            api=Mapping[table_name[t]]
            response = requests.request("POST", URL+ api, headers=headers, data=payload)
            response=response.json()
            #If the name of the table is explicit in the api, then it means the latter only 
            #shows that specific table. There is no need for additional filtering.
            if table_name[t] in api:
                #convert it to dataframe then to a csv format
                table_df = pd.DataFrame()
                for record in response:
                    df=pd.DataFrame(record, index = [0])
                    table_df=table_df.append(df)   
                response_csv = table_df.to_csv(index=False)
                #convert to json
                response_json=json.dumps(response).encode('UTF-8')
            else:
                #Select only the chosen table
                result=[]
                for n in response:
                    result.append(n[table_name[t]])
                response=result 
                #convert it to dataframe then to a csv format
                table_df = pd.DataFrame()
                for record in response:
                    df=pd.DataFrame(record, index = [0])
                    table_df=table_df.append(df)   
                response_csv = table_df.to_csv(index=False)
                #convert to json
                response_json=json.dumps(response).encode('UTF-8')
            #Transfer the data into a s3 bucket
            year=date[:4]
            month=date[5:7]
            day=date[8:10]
            s3 = boto3.resource('s3')
            s3.Object("mambu-api-rd","files/" + year + "/" + month + "/" + day + "/" + table_name[t]+ ".json").put(Body=response_json)
            s3.Object("mambu-api-rd","files/" + year + "/" + month + "/" + day + "/" + table_name[t]+ ".csv").put(Body=response_csv)
