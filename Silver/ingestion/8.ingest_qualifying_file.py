# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

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
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# specifiy schema
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# reading the data into dataframe
qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# printing the data
display(qualifying_df)

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# invoking the function for ingestion date
qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

from pyspark.sql.functions import lit

# renameing fields and adding new columns
final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format
final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# check the output is writen properly
display(spark.read.parquet("/mnt/formula1dl/processed/qualifying"))


#overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# add a exit command for exit status(in case of running all files together)
dbutils.notebook.exit("Success")



