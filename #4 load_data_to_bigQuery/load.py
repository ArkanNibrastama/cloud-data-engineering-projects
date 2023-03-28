import pandas as pd
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"

df = pd.DataFrame({

    "id" : [0,1,2,3,4,5],
    "0" : [1, 0.2, 0.4, 0.56, 0.67, 0.9],
    "1" : [0.34, 1, 0.43, 0.77, 0.12, 0.31],
    "2" : [0.75, 0.87, 1, 0.99, 0.44, 0.43],
    "3" : [0.22, 0.66, 0.43, 1, 0.80, 0.73],
    "4" : [0.05, 0.89, 0.56, 0.99, 1, 0.77],
    "5" : [0.34, 1, 0.43, 0.78, 0.12, 1],

})

bq = bigquery.Client()
table = "[YOUR PROJECT'S ID].[YOUR DATASET'S NAME].[YOUR TABLE'S NAME]"
job_conf = bigquery.LoadJobConfig(

    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE

)

load = bq.load_table_from_dataframe(df, table, job_config=job_conf)
load.result()