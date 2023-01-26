from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import glob
import os

class GCStorage:

    def __init__(self, client):
        
        self.client = client

    def create_bucket(self, bucket_name, storage_class, bucket_location='asia'):

        bucket = self.client.bucket(bucket_name)
        bucket.storage_class = storage_class
        
        return self.client.create_bucket(bucket, bucket_location)

    def get_bucket(self, bucket_name):

        return self.client.get_bucket(bucket_name)

    def list_bucket(self):

        buckets = self.client.list_buckets()

        return [bucket.name for bucket in buckets]

    def upload(self, bucket, file_name, df):

        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(), 'text/csv')

        return blob


STORAGE_CLASS = {'STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE'}
target_bucket_name = '{/YOUR BUCKET NAME}'

# change the absolute directory
os.chdir("{/PROJECT DIRECTORY}")

credentials = service_account.Credentials.from_service_account_file("./key.json")
client = storage.Client(credentials=credentials)
gcs = GCStorage(client=client)

if target_bucket_name not in gcs.list_bucket():

    print("there is no \"" + target_bucket_name + "\" bucket in your GCS!")

    # create bucket
    gcs.create_bucket(target_bucket_name, STORAGE_CLASS[0])
    print("successfull to create datalake")

else:

    print("\"" + target_bucket_name + "\" bucket has already in your GCS!")

# upload file into gcs
target_bucket = gcs.get_bucket(target_bucket_name)

path = "./dataset/"
files = glob.glob(path + "*.csv")

for c, f in enumerate(files):

    df = pd.read_csv(f)
    # print(df)

    file_name = f.split("\\")[1]
    gcs.upload(target_bucket, file_name, df)

    print(f"({c+1}/{len(files)}) successfull to upload \"{file_name}\" into GCS")


