import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True), 
    StructField("original_crime_type_name", StringType(), True), 
    StructField("report_date", StringType(), True), 
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True), 
    StructField("call_time", StringType(), True), 
    StructField("call_date_time", StringType(), True), 
    StructField("disposition", StringType(), True), 
    StructField("address", StringType(), True), 
    StructField("city", StringType(), True), 
    StructField("state", StringType(), True), 
    StructField("agency_id", StringType(), True), 
    StructField("address_type", StringType(), True), 
    StructField("common_location", StringType(), True)  
])

def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.sfopd.cs.events.1") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    print("Schema for Kafka Producer DF:")
    df.printSchema()

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    print("Schema for serive_table:")
    service_table.printSchema()
    
    # Select original_crime_type_name and disposition
    distinct_table = service_table \
        .select("original_crime_type_name", "disposition") \
        .distinct() 
    
    print("Schema for distinct_table:")
    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table \
        .dropna() \
        .select("original_crime_type_name") \
        .groupby("original_crime_type_name") \
        .agg({"original_crime_type_name" : "count"}) 

    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # Write output stream
    query = agg_df \
        .writeStream \
        .trigger(processingTime="15 seconds") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Attach a ProgressReporter
    query.awaitTermination()

    # Get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)
    
    #radio_code_df.printSchema()
    
    # Clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # Join on disposition column
    join_query = agg_df.join(radio_code_df, distinct_table.disposition == radio_code_df.disposition, "left_outer") \
        .select("original_crime_type_name", agg_df.disposition) \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .trigger(processingTime="15 seconds") \
        .start()
    
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", "3000") \
        .getOrCreate()

    #spark.sparkContext.setLogLevel("INFO")
    
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
