import requests
import json
import pandas as pd
from flatten_json import flatten, flatten_json
import csv
import numpy as np
from urllib.request import urlopen
#from json import json_normalize

#DEFINE API CALLS
#def currencies_get():
URL = "https://deloittept.sandbox.mambu.com/api/"
parameters = {
        "paginationDetails" : "ON",
        "type": "FIAT_CURRENCY"
    }
headers = {
        "Content-Type": "Application/json",
        "ApiKey":"ZzWxRf3MkRzSdjLkmLH2MVeqI9DO7WrI",
        "Accept":"application/vnd.mambu.v2+json"
    }
api = "currencies"

#REQUEST API CALL AND STORE IN CSV
response = requests.get(URL + api,headers=headers,params = parameters)
data = response.json()
data = json.dumps(response.json(),indent = 4)
print(data)
if type(data) == list:
    keys = data[0].keys()
    with open(api+'.csv', 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file,fieldnames=data[0].keys(), ) 
        fc.writeheader()
        fc.writerows(data)



#data = response.json()
#print(data['response'])
#response = requests.get(URL + api,headers=headers,params = parameters)
#print(response)
#print(type(data))
#data = response.json()
#print(response['response'])
#data = json.loads(response.read())
#data[0]

#df = pd.DataFrame(data) 
#np.savetxt("GFG.json", 
##           data,
#           delimiter =", ", 
#           fmt ='% s')
#compression_opts = dict(method='zip',
#                        archive_name='out.csv')  
#df.to_csv('out.zip', index=False,
#          compression=compression_opts)
#data = map(lambda each:each.strip("{"), data)
#print(data)


#data.replace('{','', inplace = True)
#np.savetxt("GFG.csv", 
#           data,
#           delimiter =", ", 
#           fmt ='% s')

#df = pd.DataFrame(data) 
#df.head()




#data = json.dumps(response.json(),indent = 4)
#print(type(data))#data_2 = response.json()
#DataDict = json.loads(data)
#print(type(DataDict))
#print(type(data))
#flat_json = pd.DataFrame([flatten_json(data[key])] for key in data)
#print(flat_json)
