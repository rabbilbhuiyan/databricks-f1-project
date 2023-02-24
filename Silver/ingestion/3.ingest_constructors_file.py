# MAGIC %md
# MAGIC ### Ingest constructors.json file

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
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# construct DDL schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
# reading data
constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# displaying data
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

# adding ingestion date using the user defined function
constructor_final_df = add_ingestion_date(constructor_renamed_df)

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file
constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/constructors")

#constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# Check the saving of output
# %fs
# ls /mnt/formula1dl/processed/constructors

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")