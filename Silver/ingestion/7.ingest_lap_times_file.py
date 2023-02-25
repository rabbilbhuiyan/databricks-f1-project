# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

## parametrize with data source name and getting the value using widgets
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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# specify schema
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# reading the data into dataframe
lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# printing data
display(lap_times_df)

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# revoking the functions for adding ingestin date
lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

from pyspark.sql.functions import lit

# renaming columns and adding new columns
final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format
final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# check the output is writen properly
display(spark.read.parquet("/mnt/formula1dl/processed/lap_times"))


#overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# write the data in the database (f1_processed) as saveAsTable method
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.tap_times")


#merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
#merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# add a exit command for exit status(in case of running all files together)
dbutils.notebook.exit("Success")