import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# https://github.com/pysql-beam/pysql-beam
from pysql_beam.sql_io.sql import ReadFromSQL
from pysql_beam.sql_io.wrapper import MySQLWrapper
import logging

# transformation 
# -> replace 'DZN' from product_capacity
class Transform(beam.DoFn):

    def process(self, data):

        # replace value
        data['product_capacity'] = data['product_capacity'].replace(' DZN', '')\
                                    .replace('DZN', '').replace(' Dozen', '')\
                                    .replace('Kgs/Mont','')


        # nulll handling & convert data type
        if data['contact_person'] == 'nan' : data['contact_person'] = None
        if data['district'] == 'nan' : data['district'] = None
        if data['email_address'] == 'nan' : data['email_address'] = None
        if data['mailing_address'] == 'nan' : data['mailing_address'] = None
        if data['company_name'] == 'nan' : data['company_name'] = None
        if data['mobile_number'] == 'nan' : data['mobile_number'] = None
        if data['product_capacity'] != 'nan' : data['product_capacity'] = float(data['product_capacity'])
        else : data['product_capacity'] = None
        if data['no_machines'] != 'nan' : data['no_machines'] = int(data['no_machines'])
        else : data['no_machines'] = None
        if data['no_employees'] != 'nan' : data['no_employees'] = int(data['no_employees'])
        else : data['no_employees'] = None
        
        return [data]

opt = PipelineOptions(

    runner='DirectRunner',
    temp_location='[YOUR_GCS_TEMP_PATH]'

)

def data_migration():

    with beam.Pipeline(options=opt) as p:

        (

            p
            |'Extract data from mysql' >> ReadFromSQL(
            
                host='localhost',
                port='3306',
                username='root',
                password='',
                database='members',
                query='SELECT * FROM members;',
                wrapper=MySQLWrapper

            )
            |'Transform the data' >> beam.ParDo(Transform())
            # |beam.Map(print)
            |'Load into BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
            
                    table='members',
                    dataset='members',
                    project='[YOUR_GCP_PROJECT]',
                    # schema=('bgmea_reg_no:INTEGER,contact_person:STRING,district:STRING,\
                    #         email_adddress:STRING,mailing_address:STRING,company_name:STRING,\
                    #         mobile_number:STRING,no_employees:INTEGER,no_machines:INTEGER,\
                    #         product_capacity:FLOAT'
                    #         ),
                    schema=beam.io.SCHEMA_AUTODETECT,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

                )
            )

        )

if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
    data_migration()