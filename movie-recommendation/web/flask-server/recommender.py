import os
import numpy as np
import json
import random
from google.cloud import bigquery, storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"
bq = bigquery.Client()
gcs = storage.Client()
bucket = gcs.bucket('{YOUR BUCKET NAME}')

def show_movies(n_movies):

    show_movies = []

    for i in range(n_movies):

        gen_id = random.randint(0,999)
        gen_id = "%04d" % gen_id

        blob = bucket.blob(f'movie/movie-{gen_id}')
        mov = json.loads(blob.download_as_string())
        
        show_movies.append(mov)

    return show_movies

def get_product_recommendation(clicked_id, n_show):

    for i in range(10):

        if clicked_id in range(i*100, (i*100)+100):

            part = i+1
            break

    query = f'''

        SELECT id
        FROM movie_recommendation.recommendation_table_part_{part}
        ORDER BY `{clicked_id}` DESC
        LIMIT {n_show+1};

    '''

    q = bq.query(query).to_dataframe()

    rec_movies = []
    for mov_id in np.array(q):

        num = "%04d" % mov_id
        blob = bucket.blob(f'movie/movie-{num}')
        mov = json.loads(blob.download_as_string())
        
        rec_movies.append(mov)

    return rec_movies