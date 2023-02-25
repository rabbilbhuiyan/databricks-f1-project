# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# parametrize with data source name and getting the value using widgets
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# parameterize with file date and getting the value using widgets
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# invoke notebook with configuration parameter to avoid hardcoding of path folder
MAGIC %run "../set-up/configuration"

# invoke notebook with function
MAGIC %run "../functions/common_functions"


# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# specify schema
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# reading the data into dataframe
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# printing the data
display(pit_stops_df)

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns


# revoking the functions for ingestion date
pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

from pyspark.sql.functions import lit

# rename and adding ngestion_date with current timestamp
final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format
final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# check the output is writen properly
display(spark.read.parquet("/mnt/formula1dl/processed/pit_stops"))

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')


merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# add a exit command for exit status(in case of running all files together)
dbutils.notebook.exit("Success")


# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;


