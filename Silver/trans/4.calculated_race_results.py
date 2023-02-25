# Databricks notebook source
# parameterize with file date and getting the value using widgets
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

USE f1_processed;


# create the calculated race results table 
CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM results 
  JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_processed.races ON (results.race_id = races.race_id)
 WHERE results.position <= 10


# query the table
SELECT * FROM f1_presentation.calculated_race_results


