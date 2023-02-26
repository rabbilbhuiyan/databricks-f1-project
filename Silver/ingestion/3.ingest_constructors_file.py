# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# parametrize with data source name and getting the value using widgets
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# parameterize with file date and getting the value using widget
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# invoke notebook with configuration parameter to avoid hardcoding of path folder
MAGIC %run "../set-up/configuration"

# invoke notebook with function
MAGIC %run "../functions/common_functions"

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# construct DDL schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# reading data into dataframe
constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# printing data
display(constructor_df)

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

from pyspark.sql.functions import col
constructor_dropped_df = constructor_df.drop(col('url'))

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

from pyspark.sql.functions import lit
constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# revoking using defined function
constructor_final_df = add_ingestion_date(constructor_renamed_df)

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file
constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# check whether the output is writen properly 
display(spark.read.parquet("/mnt/formula1dl/processed/constructors"))

# write the data in the database (f1_processed) as saveAsTable method
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# query the table
# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# add a exit command for exit status(in case of running all files together)
dbutils.notebook.exit("Success")