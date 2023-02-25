# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

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


#MAGIC %md
#MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# import data types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# specify schema
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# reading data into dataframe
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# printing data
display(circuits_df)

# printing schema
circuits_df.printSchema()


# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# importing libraries
from pyspark.sql.functions import col

# dataframe with selected columns
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))


# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# importing libraries
from pyspark.sql.functions import lit

# dataframe with renaming and adding data source and file date
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# revoking the user defined function for ingestion date
circuits_final_df = add_ingestion_date(circuits_renamed_df)


# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet file

# wirte data using DataFrameWriter api
circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# check the data is writen by listing the file system
# %fs
# ls /mnt/formula1dl/processed/circuits

# check whether the output is writen properly 
display(spark.read.parquet("/mnt/formula1dl/processed/circuits"))


# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# add a exit command for exit status(in case of running all files together)
dbutils.notebook.exit("Success")