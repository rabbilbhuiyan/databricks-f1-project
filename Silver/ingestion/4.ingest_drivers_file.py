# MAGIC %md
# MAGIC ### Ingest drivers.json file

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
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# define schema for inner json objects
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# define schema for outer json objects
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# reading the data into dataframe
drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# displaying the dataframe
display(drivers_df)

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# driverId renamed to driver_id  
# driverRef renamed to driver_ref  
# ingestion date added
# name added with concatenation of forename and surname

from pyspark.sql.functions import col, concat, lit

# adding ingestion date using user defined function
drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))


# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format
drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/drivers")

#drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")