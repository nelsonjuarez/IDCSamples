import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import logging
import boto3
import re

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_parameters():
    return getResolvedOptions(sys.argv, ['JOB_NAME', 'TABLE_NAME', 'LANDING_PATH', 'RAW_PATH', 'DATABASE_NAME'])

def initialize_glue_job(args):
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    return sc, glueContext, spark, job

def read_data(spark, source_path):
    try:
        df = spark.read.csv(source_path, header=True, inferSchema=True)
        logger.info("Data read successfully.")
        return df
    except Exception as e:
        logger.error(f"Error reading data from {source_path}: {str(e)}")
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
    landing_path = args['LANDING_PATH']
    raw_path = args['RAW_PATH']
    database_name = args['DATABASE_NAME']
    
    sc, glueContext, spark, job = initialize_glue_job(args)
    
    logger.info(f"Job {args['JOB_NAME']} started.")
    
    create_database_if_not_exists(database_name)
    
    source_path = landing_path
    logger.info(f"Reading data from {source_path}")
    
    df = read_data(spark, source_path)
    
    logger.info("Sanitizing data...")
    sanitized_df = sanitize_data(df)
    logger.info("Data sanitized successfully.")
    
    logger.info("Saving data to Glue Data Catalog...")
    save_to_catalog(glueContext, sanitized_df, database_name, table_name, raw_path)
    
    job.commit()
    logger.info(f"Job {args['JOB_NAME']} completed successfully.")

if __name__ == "__main__":
    main()
