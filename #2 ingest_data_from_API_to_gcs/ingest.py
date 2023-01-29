import os
import json
import requests
import pandas as pd
from datetime import datetime
import re

from google.cloud import storage
from google.oauth2 import service_account
import gcsfs

class GCS:

    def __init__(self, client) -> None:
        
        self.client = client

    # bucket function
    def get_bucket(self, name_bucket):

        return self.client.bucket(name_bucket)

    def list_bucket(self):

        buckets = self.client.list_buckets()

        return [bucket.name for bucket in buckets]

    def create_bucket(self, name_bucket, class_storage = 'STANDARD', region = "asia"):

        bucket = self.client.bucket(name_bucket)
        bucket.class_storage = class_storage

        return self.client.create_bucket(bucket, location=region)

    # object / file function
    def list_object(self, bucket_name, prefix=None, delimiter=None, name=True):

        # if name is true then return dict of string, but if false then return dict of object(blob)
        returnValue = None

        blobs = self.client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

        if name == True:

            returnValue = [blob.name for blob in blobs]

        else:

            returnValue = [blob for blob in blobs]

        return returnValue

    def upload(self, bucket, filename, data, type='json'):

        file_format = ''

        if type == 'csv':

            file_format = 'text/csv'

        else:

            file_format = 'application/json'

        blob = bucket.blob(f'{type}/{filename}')
        return blob.upload_from_string(data, file_format)

    def remove(self, bucket, filename='data.csv', type='csv'):

        blob = bucket.blob(f'{type}/{filename}')
        return blob.delete()

    

# set up gcs
print('set up gcs ...')
os.chdir("{/YOUR FILE PROGRAM PATH}")
credentials = service_account.Credentials.from_service_account_file('./gcs_admin.json')
client = storage.Client(credentials=credentials)
gcs = GCS(client=client)

# bucket initialization
print('bucket initialization ...')
bucket_name = '{/YOUR BUCKET NAME}'
if bucket_name not in gcs.list_bucket():

    gcs.create_bucket(bucket_name)

bucket = gcs.get_bucket(bucket_name)


# get json data from data source
print("get the data ...")
URL = "https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json"
response = requests.get(URL)
json_text = response.text

data = json.loads(json_text)
data = data['Infogempa']['gempa']
filename = data['Tanggal']+" "+data['Jam']+".json"

# if bucket is empty or data is different, then upload the data
if f'json/{filename}' not in gcs.list_object(bucket_name):

    print('uploading json file ...')

    # upload data into datalake/json/
    gcs.upload(bucket=bucket, filename=filename, data=json.dumps(data), type='json')

    # transformation
    time = datetime.strptime(data['DateTime'], "%Y-%m-%dT%H:%M:%S+00:00")
    coordinates = data['Coordinates'].split(',')
    x = float(coordinates[0])
    y = float(coordinates[1])
    lintang = data['Lintang']
    bujur = data['Bujur']
    magnitude = float(data['Magnitude'])
    kedalaman = float(data['Kedalaman'].replace(' km', ''))
    wilayah = data['Wilayah'].split(' ')
    area = wilayah[4]
    location = " ".join(wilayah[5:])
    potensi = data['Potensi']

    csv_data = [time, x, y, lintang, bujur, magnitude, kedalaman, area, location, potensi]

    # append data into datalake/csv/data.csv
    # if folder is empty
    print('uploading csv file ...')
    if f'csv/data.csv' not in gcs.list_object(bucket_name=bucket_name, prefix='csv'):
        
        column = ['datetime', 'Xcoordinate', 'Ycoordinate', 'LS', 'BT',
        'magnitude', 'kedalaman(km)', 'area', 'lokasi', 'potensi']

        df = pd.DataFrame(data=[csv_data], columns=column)

        gcs.upload(bucket=bucket, filename='data.csv', data=df.to_csv(), type='csv')

    # if file is existing
    else:

        df = pd.read_csv("{/gs://{YOUR BUCKET NAME}/csv/data.csv}")

        # append records
        df.loc[len(df)] = csv_data

        # delete old file
        gcs.remove(bucket=bucket, filename='data.csv', type='csv')

        # upload new dataframe 
        gcs.upload(bucket=bucket, filename='data.csv', data=df.to_csv(index=False), type='csv')

else:

    print('file is same as before ...')