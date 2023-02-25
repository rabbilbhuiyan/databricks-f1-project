# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# parameterize with file date and getting the value using widgets
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# invoke notebook with configuration parameter to avoid hardcoding of path folder
MAGIC %run "../set-up/configuration"

# invoke notebook with function
MAGIC %run "../functions/common_functions"

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

race_results_df = spark.read.format("parquet").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

race_year_list = df_column_to_list(race_results_df, 'race_year')


from pyspark.sql.functions import col

race_results_df = spark.read.format("parquet").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# creating driver standing dataframe

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# creating driver rank

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# write the data into presentation layers
final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# write the data in the database (f1_presentation) as saveAsTable method
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")


#merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
#merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# query the table
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings WHERE race_year = 2021;


# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC  GROUP BY race_year
# MAGIC  ORDER BY race_year DESC;


