
pip install azure-common azure-nspkg azure-storage-common azure-storage-common

pip install azure-storage-blob==0.37.1


from azure.storage.blob import BlockBlobService

block_blob_service = BlockBlobService(account_name='pmcovidstorage', account_key='4Ufd30qimeVHKioZcFzZx33PF+4MbrmT0/AwOEZTqDE6zZ6RDuZnmRjrgU52Gl6bIBLLM35dZcC+A1Gi3cCl/w==')
block_blob_service.get_blob_to_path('coviddatasetsblobcontainer','ourworldin.full_data.csv','ourworldin.full_data.csv')
import pandas as pd 
dataframe_blobdata = pd.read_csv('ourworldin.full_data.csv')
dataframe_blobdata.head(10)

name='United States'

data_location=dataframe_blobdata[dataframe_blobdata['location']==name]

data_location.shape
data_location.isna().sum()

deaths=data_location.loc[:,['total_deaths','date']]
import urllib,json


import urllib.request, json 
with urllib.request.urlopen("https://opendata.ecdc.europa.eu/covid19/casedistribution/json/") as url:
    data = json.loads(url.read().decode())
    print(data)

import pandas as pd
df = pd.DataFrame.from_dict(data, orient='columns')

df.head()

#so here the data is nested so i will use normalizin

from pandas.io.json import json_normalize

df = pd.DataFrame.from_dict(json_normalize(data["records"]), orient='columns')


df_b=df.to_string()

block_blob_service.create_blob_from_text('coviddatasetsblobcontainer', 'OutFile.csv', df_b)  

data_file = open('data_file.csv', 'w') 
  
# create the csv writer object 
csv_writer = csv.writer(data_file) 
  
# Counter variable used for writing  
# headers to the CSV file 
count = 0
  
for emp in df: 
    if count == 0: 
  
        # Writing headers of CSV file 
        header = emp.keys() 
        csv_writer.writerow(header) 
        count += 1
  
    # Writing data of CSV file 
    csv_writer.writerow(emp.values()) 
  
data_file.close() 