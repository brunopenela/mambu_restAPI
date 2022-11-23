import boto3
from datetime import datetime,date, timedelta
import re

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

#function retrieve tables to be processed
def get_tables_to_process (config_file_data):
    return re.findall(r'\S+', config_file_data['tables']) 
