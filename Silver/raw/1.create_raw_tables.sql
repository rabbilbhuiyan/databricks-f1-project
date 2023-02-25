-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- MAGIC %md
-- MAGIC #### Create tables for CSV files


-- MAGIC %md
-- MAGIC ##### Create circuits table

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/circuits.csv", header true)

# query the table
SELECT * FROM f1_raw.circuits;

-- MAGIC %md
-- MAGIC ##### Create races table

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/races.csv", header true)

# query the table
SELECT * FROM f1_raw.races;

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- MAGIC %md
-- MAGIC ##### Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/constructors.json")

# query the table
SELECT * FROM f1_raw.constructors;

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/formula1dl/raw/drivers.json")

-- MAGIC %md ##### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/results.json")

# query the table
SELECT * FROM f1_raw.results

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/pit_stops.json", multiLine true)

# query the table 
SELECT * FROM f1_raw.pit_stops;

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dl/raw/lap_times")

# query the table
SELECT * FROM f1_raw.lap_times

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/formula1dl/raw/qualifying", multiLine true)

# query the table
SELECT * FROM f1_raw.qualifying

# describe the external table 
DESC EXTENDED f1_raw.qualifying;



