
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

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

# reading data using spark
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# displaying dataframe
display(circuits_df)

# printing schema
circuits_df.printSchema()


# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# importing libraries
from pyspark.sql.functions import col

# dataframe with selected columns
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# displaying df
display(circuits_selected_df)

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# importing libraries
from pyspark.sql.functions import lit

# dataframe with renaming
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# displaying df
display(circuits_renamed_df)

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# calling the function 'add_ingestion_date' (in functions folder)
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# displaying the df
display(circuits_final_df)

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet file

# wirte data using DataFrameWriter api
circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/circuits")

# circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# check the data is writen by listing the file system
# %fs
# ls /mnt/formula1dl/processed/circuits

# validation of saved data by reading the data 
df=spark.read.parquet("/mnt/formula1dl/processed/circuits")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")