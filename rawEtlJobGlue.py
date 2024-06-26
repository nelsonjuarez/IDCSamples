import sys
import requests
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import logging
import boto3
import re
import json

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_parameters():
    return getResolvedOptions(sys.argv, ['JOB_NAME', 'TABLE_NAME', 'API_URL', 'API_KEY', 'RAW_PATH', 'DATABASE_NAME'])

def initialize_glue_job(args):
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    return sc, glueContext, spark, job

def get_data_from_api(api_url, api_key):
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError on bad response
        logger.info("Data fetched successfully from API.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {str(e)}")
        raise

def read_data_from_api(spark, api_url, api_key):
    data = get_data_from_api(api_url, api_key)
    
    # Log the fetched data for debugging
    logger.info(f"Fetched data: {json.dumps(data, indent=2)}")

    try:
        # Flatten the JSON structure to a list of records for Spark DataFrame
        records = [{"currency": k, "rate": float(v), "timestamp": data["timestamp"], "base": data["base"]}
                   for k, v in data["rates"].items()]

        # Convert the list of records to a Spark DataFrame
        df = spark.createDataFrame(records)
        logger.info("Data read successfully from API.")
        return df
    except Exception as e:
        logger.error(f"Error processing data from API: {str(e)}")
        raise

def sanitize_data(df):
    reserved_keywords = {"select", "from", "where", "group", "order", "limit"}
    for col in df.columns:
        new_col = re.sub(r'[^\w]', '_', col).lower()
        if re.match(r'^\d', new_col) or new_col in reserved_keywords:
            new_col = f"_{new_col}"
        df = df.withColumnRenamed(col, new_col)
    return df

def create_database_if_not_exists(database_name):
    client = boto3.client('glue')
    try:
        client.get_database(Name=database_name)
        logger.info(f"Database {database_name} already exists.")
    except client.exceptions.EntityNotFoundException:
        logger.info(f"Database {database_name} does not exist. Creating database.")
        client.create_database(DatabaseInput={'Name': database_name})

def save_to_catalog(glueContext, df, database, table_name, path):
    try:
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
        s3output = glueContext.getSink(
            path=path,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            enableUpdateCatalog=True,
            partitionKeys=[],
            transformation_ctx="s3output"
        )
        s3output.setFormat("glueparquet", compression="snappy")
        s3output.setCatalogInfo(catalogDatabase=database, catalogTableName=table_name)
        s3output.writeFrame(dynamic_frame)
        logger.info(f"Table {table_name} created or updated successfully in Glue Data Catalog.")
    except Exception as e:
        logger.error(f"Error creating or updating table in Glue Data Catalog: {str(e)}", exc_info=True)
        raise

def main():
    args = get_parameters()
    
    table_name = args['TABLE_NAME']
    api_url = args['API_URL']
    api_key = args['API_KEY']
    raw_path = args['RAW_PATH']
    database_name = args['DATABASE_NAME']
    
    sc, glueContext, spark, job = initialize_glue_job(args)
    
    logger.info(f"Job {args['JOB_NAME']} started.")
    
    create_database_if_not_exists(database_name)
    
    logger.info(f"Fetching data from API: {api_url}")
    
    df = read_data_from_api(spark, api_url, api_key)
    
    logger.info("Sanitizing data...")
    sanitized_df = sanitize_data(df)
    logger.info("Data sanitized successfully.")
    
    # Truncating destination bucket)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(raw_path.split('/')[2])
    prefix = '/'.join(raw_path.split('/')[3:])
    
    for obj in bucket.objects.filter(Prefix=prefix):
        s3.Object(bucket.name, obj.key).delete()
    
    sanitized_df.write.mode("overwrite").parquet(raw_path)
    
    logger.info("Saving data to Glue Data Catalog...")
    save_to_catalog(glueContext, sanitized_df, database_name, table_name, raw_path)
    
    job.commit()
    logger.info(f"Job {args['JOB_NAME']} completed successfully.")

if __name__ == "__main__":
    main()
