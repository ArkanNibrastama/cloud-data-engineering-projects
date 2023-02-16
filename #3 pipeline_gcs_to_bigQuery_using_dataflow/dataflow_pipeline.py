import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import os
from datetime import datetime
import re
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./key.json"

# service account:
# storage admin, dataflow admin, dataflow worker, data pipeline admin, data pipeline viewer,
# data pipeline invoker, bigquery admin
# grant user:
# service account admin | owner

class transform(beam.DoFn):

    def process(self, data):

        data = json.loads(data)

        time = datetime.strptime(data['DateTime'], "%Y-%m-%dT%H:%M:%S+00:00")
        strtime = time.strftime("%Y-%m-%d %H:%M:%S")

        coordinates = data['Coordinates'].split(',')
        x = float(coordinates[0])
        y = float(coordinates[1])

        lintang = data['Lintang']
        bujur = data['Bujur']

        magnitude = float(data['Magnitude'])
        kedalaman = float(data['Kedalaman'].replace(' km', ''))

        if 'darat' in data['Wilayah'].lower():

            area = 'darat'

        elif 'laut' in data['Wilayah'].lower():

            area = 'laut'

        else:

            # set default area to darat
            area = 'darat'

        lokasi = re.split(r'barat daya |barat laut |timur laut |tenggara |kabupaten |kab. |kota |selatan |barat |utara |timur |baratdaya |baratlaut |timurlaut ', data['Wilayah'].lower())[-1]
        lokasi = lokasi.split('-')[0]
        lokasi = lokasi.upper()

        potensi = data['Potensi']
        
        return [[strtime, x, y, lintang, bujur, magnitude, kedalaman, area, lokasi, potensi]]

def parse_bigquery_readable(data):

    formated = dict(zip(

        ('timestamp', 'XCoordinate', 'YCoordinate', 'lintang', 'bujur', 'magnitude',
        'kedalaman', 'area', 'location', 'potensi'),
        data

    ))

    return formated

# pipeline options
opt = PipelineOptions(
    save_main_session = True,
    runner = 'DataflowRunner',
    job_name='{/YOUR JOBS NAME}',
    project = "{YOUR PROJECTS ID}",
    temp_location = "gs://{/YOUR BUCKET}/temp/",
    region ="{YOUR REGION}",
    staging_location = 'gs://{/YOUR BUCKET}/stage/',
    # if you want to create data pipeline on dataflow
    # template_location = 'gs://{/YOUR BUCKET}/template/template.json',
    service_account_email = "{YOUR SERVICE ACCOUNT EMAIL}"
)

# Pcollections
def run_pipeline():

    with beam.Pipeline(options=opt) as pipe:

        (
            pipe
            |'Read CSV File From GCS'>>beam.io.ReadFromText("gs://{YOUR BUCKET}/json/")
            |'Transform The Data' >> beam.ParDo(transform())
            |'Convert Into BigQuery Schema' >> beam.Map(lambda r : parse_bigquery_readable(r))
            # |beam.Map(print)
            |'Load Into BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    table='{TABLE NAME}',
                    dataset='{DATASET NAME}',
                    project='{YOUR PROJECT ID}',
                    schema=(

                        'timestamp:TIMESTAMP,XCoordinate:FLOAT,YCoordinate:FLOAT,\
                        lintang:STRING,bujur:STRING,magnitude:FLOAT,kedalaman:FLOAT,\
                        area:STRING,location:STRING,potensi:STRING'

                    ),
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
            )
        )

    # if not use "with beam.pipeline ..."
    # and use pipe = beam.pipeline(pipe| ...)
    # then, to run it you need to use :
    # pipe.run().wait_until_finish()



if __name__=="__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
