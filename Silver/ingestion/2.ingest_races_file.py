# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# import the data types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# specify the schema
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# read the data into dataframe
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# printing the data
display(races_df)


# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# importing the necessary libraries
from pyspark.sql.functions import to_timestamp, concat, col, lit

# adding the required columns
races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# revoking the user defined functions
races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)


# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# selecting the required columns
races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                        col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))


# MAGIC %md
# MAGIC ##### Step 4 - Write the output to processed container in parquet format
races_selected_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")

# check whether the output is writen properly 
display(spark.read.parquet("/mnt/formula1dl/processed/races"))

# write the data in the database (f1_processed) as saveAsTable method
races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# query the table
# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# add a exit command for exit status(in case of running all files together)
dbutils.notebook.exit("Success")