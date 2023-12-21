import boto3
import snowflake.connector

###sangamesh::This function will call the snowflake procedure, please note that this code snippet is at very high level which can be leveraged to capture details at next level
###sangamesh::a layer needs to be created in this lambda function to accomodate snowflake connector library.

def lambda_handler(event, context):
try:
     s3_bucket = event['Records'][0]['s3']['bucket']['name']
     s3_key = event['Records'][0]['s3']['object']['key']
     
    # snowflake connection details
     sf_user = 'mr_admin_demo'
     sf_password = 'mr_admin123_demo'  ##sangamesh:: credential can also be stored and retrieved from secrets manager.
     sf_account = 'mr_demo'
     sf_database = 'mr_db_demo'
     sf_schema = 'mr_unrefined_demo'
     sf_stage = 'mr_stage_demo'
     
    ##sangamesh:: create a snowflake connection
     sf_conn = snowflake.connector.connect(
     user=sf_user,
     password=sf_password,
     account=sf_account,
     database=sf_database,
     schema=sf_schema,
     warehouse='mr_wh'
     )
     
    ##sangamesh:: invoke the snowflake procedure which will have the copy into command integrated into it and also preprocess/postprocess audit functionality
     copy_query = f"call {schema}.s3_to_snowflake_ingestion_procedure()"
     sf_conn.cursor().execute(copy_query)
     
    ##sangamesh:: close the sf connection
     sf_conn.close()
except ValueError as e:
        print('s3 to snowflake ingestion lambda failed please investigate:' % e)
        return None