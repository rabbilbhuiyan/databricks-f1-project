# Databricks notebook source
# parameterize with file date and getting the value using widgets
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# invoke notebook with configuration parameter to avoid hardcoding of path folder
MAGIC %run "../set-up/configuration"

# invoke notebook with function
MAGIC %run "../functions/common_functions"

# MAGIC %md
# MAGIC ##### Read all the data as required

drivers_df = spark.read.format("parquet").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

constructors_df = spark.read.format("parquet").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

circuits_df = spark.read.format("parquet").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

races_df = spark.read.format("parquet").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

results_df = spark.read.format("parquet").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date") 


# MAGIC %md
# MAGIC ##### Join circuits to races

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)


from pyspark.sql.functions import current_timestamp

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date") 

# write the data into presentation layers
final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# write the data in the database (f1_presentation) as saveAsTable method
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")


# query the table
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;



