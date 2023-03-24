from google.cloud import storage
import os
import pandas as pd
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./key.json"


gcs = storage.Client()
bucket = gcs.bucket(bucket_name='{YOUR BUCKET NAME}')

df = pd.read_csv('./dataset/imdb_top_1000.csv')

# field name
key = df.columns.values


for i in range(len(df)):

    # record value of i-th row
    values = df.loc[i].values

    movie = dict()

    movie['id'] = i
    for k, v in zip(key, values):

        movie[k] = str(v)

    print("="*100)
    print(json.dumps(movie))

    num = "%04d" % i
    blob = bucket.blob(f'movie/movie-{num}')
    blob.upload_from_string(json.dumps(movie), 'application/json')

    print("Upload Success!")
    print("="*100)
