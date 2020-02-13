#  /usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 data_stream.py
import logging
from pathlib import Path

import pyspark.sql.functions as psf
from config import BROKER_URL, CRIME_DATA_TOPIC_NAME
from pyspark.sql import SparkSession
from pyspark.sql.types import *

logger = logging.getLogger(__name__)

schema = StructType([StructField("crime_id", StringType()),
                     StructField("original_crime_type_name", StringType()),
                     StructField("report_date", StringType()),
                     StructField("call_date", StringType()),
                     StructField("offence_date", StringType()),
                     StructField("call_time", StringType()),
                     StructField("call_date_time", StringType()),
                     StructField("disposition", StringType()),
                     StructField("address", StringType()),
                     StructField("city", StringType()),
                     StructField("state", StringType()),
                     StructField("agency_id", StringType()),
                     StructField("address_type", StringType()),
                     StructField("common_location", StringType())])


def run_spark_job(spark):
    # Create Spark configurations with max offset of 200 per trigger
    logger.info(f"Broker URL: {BROKER_URL} Topic: {CRIME_DATA_TOPIC_NAME}")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER_URL) \
        .option("subscribe", CRIME_DATA_TOPIC_NAME) \
        .option("maxOffsetsPerTrigger", 200) \
        .load()

    # Extract JSON keys as columns of service_table data frame
    service_table = df.withColumn('data', psf.from_json(psf.col("value").cast('string'), schema)).select('data.*')

    print("service_table schema")
    service_table.printSchema()

    # Select original crime type name and disposition
    distinct_table = service_table.select('original_crime_type_name', 'disposition')

    # count the number of original crime type
    agg_df = distinct_table.groupBy(['original_crime_type_name', 'disposition']).count()
    print("agg_df schema")
    agg_df.printSchema()

    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)
    print("radio_code_df schema")
    radio_code_df.printSchema()

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    # radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    joined_df = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition_code, 'inner')

    query = joined_df \
        .select('original_crime_type_name', 'description', 'count') \
        .orderBy('count', ascending=False) \
        .writeStream \
        .format('console') \
        .outputMode('complete') \
        .start()

    query.awaitTermination()


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("KafkaSparkStructuredStreaming") \
    .getOrCreate()

logger.info("Spark started")

run_spark_job(spark)

spark.stop()
