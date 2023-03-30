import apache_beam as beam
from apache_beam.pipeline import PipelineOptions
import logging
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./key.json"

opt = PipelineOptions(
    save_main_session = True,
    runner = 'DataflowRunner',
    job_name='[YOUR JOB NAME]',
    project = "[YOUR PROJECT ID]",
    region ="[YOUR REGION]",
    temp_location = "[YOUR TEMP STORAGE]",
    service_account_email = "[YOUR SERVICE ACCOUNT]"
)

class transform(beam.DoFn):

    def process(self, data):

        data = json.loads(data)

        movie_id = data['id']
        poster = data['Poster_Link']
        title = data['Series_Title']
        release_year = data['Released_Year']
        duration_minute = data['Runtime'].split(" min")[0]
        genre = data['Genre']
        imdb_rating = data['IMDB_Rating']
        meta_score = data['Meta_score']
        sinopsis = data['Overview']

        return [[movie_id, poster, title, release_year, duration_minute, genre, imdb_rating, meta_score, sinopsis]]

def transform_bq_schema(data):

    formatted = dict(zip(

        ['movie_id','poster','title','release_year','duration_minute','genre','imdb_rating',
         'meta_score','sinopsis'],
        data

    ))

    return formatted

def etl_movie_recommendation():

    with beam.Pipeline(options=opt) as pipe:

        (

            pipe
            |"Read JSON file from datalake" >> beam.io.ReadFromText("[YOUR DATA LAKE]")
            |"Transform data" >> beam.ParDo(transform())
            |beam.Map(lambda x : transform_bq_schema(x))
            |"Load into BigQuery" >> beam.io.Write(
            
                beam.io.WriteToBigQuery(
            
                    table="movie",
                    dataset="movie_recommendation",
                    project="[YOUR PROJECT ID]",
                    schema=("movie_id:INTEGER,poster:STRING,title:STRING,release_year:STRING,\
                            duration_minute:INTEGER,genre:STRING,imdb_rating:FLOAT,meta_score:FLOAT,\
                            sinopsis:STRING"),
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

                )

            )

        )


if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
    etl_movie_recommendation()
