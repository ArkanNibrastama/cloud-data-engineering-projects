from google.cloud import bigquery
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./key.json"

bq = bigquery.Client()
query = '''
    
    SELECT movie_id, sinopsis
    FROM movie_recommendation.movie
    ORDER BY movie_id;

'''

result = bq.query(query).to_dataframe()
X = np.array(result['sinopsis'])

text_to_vec = SentenceTransformer('distilbert-base-nli-mean-tokens')
embeddings = text_to_vec.encode(X, show_progress_bar=True)

df = pd.DataFrame(cosine_similarity(embeddings))
df.columns = df.columns.astype(str)

df_partitions = []
n_partition = 10
n_field_per_partition = int(df.shape[1] / n_partition)

for i in range(n_partition):
        
        r = i*n_field_per_partition
        df_partition = df.copy()
        df_partition = df_partition.iloc[:, r : r+n_field_per_partition]
        df_partition['id'] = result['movie_id']
        df_partitions.append(df_partition)


list_table = [table.dataset_id+'.'+table.table_id for table in bq.list_tables('movie_recommendation')]
for n, df in enumerate(df_partitions):
    
    table = f"movie_recommendation.recommendation_table_part_{n+1}"

    if table not in list_table:

        bq.create_table(table)

    job_conf = bigquery.LoadJobConfig(

        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE

    )

    load = bq.load_table_from_dataframe(df, table, job_config=job_conf)
    load.result()
    
    print(f'table {n+1} loaded successfully!')