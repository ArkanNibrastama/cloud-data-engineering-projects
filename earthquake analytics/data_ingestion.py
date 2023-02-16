import os
import json
import requests

from google.cloud import storage
from google.oauth2 import service_account

class GCS:

    def __init__(self, client) -> None:
        
        self.client = client

    # bucket function
    def get_bucket(self, name_bucket):

        return self.client.bucket(name_bucket)

    def list_bucket(self):

        buckets = self.client.list_buckets()

        return [bucket.name for bucket in buckets]

    def create_bucket(self, name_bucket, class_storage = 'STANDARD', region = "asia-southeast2"):

        bucket = self.client.bucket(name_bucket)
        bucket.class_storage = class_storage

        return self.client.create_bucket(bucket, location=region)

    # object / file function
    def list_object(self, bucket_name, prefix=None, delimiter=None):

        blobs = self.client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

        return [blob.name for blob in blobs]

    def upload(self, bucket, filename, data):

        blob = bucket.blob(f'{type}/{filename}')
        return blob.upload_from_string(data, 'application/json')

# set up gcs
print('set up gcs ...')
credentials = service_account.Credentials.from_service_account_file('./key.json')
client = storage.Client(credentials=credentials)
gcs = GCS(client=client)

# bucket initialization
print('bucket initialization ...')
bucket_name = '{YOUR BUCKET NAME}'
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

    try:

        print('uploading json file ...')

        # upload data into datalake/json/
        gcs.upload(bucket=bucket, filename=filename, data=json.dumps(data), type='json')

    except Exception as e:

        print("error while uploading file : "+e)

else:

    print('file is same as before ...')