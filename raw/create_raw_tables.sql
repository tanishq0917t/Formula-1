-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Create tables for CSV files

-- COMMAND ----------

create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create circuits table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string
) using csv
options (path "/mnt/tanishqformula1dl/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races (
  raceId int,
  year int,
  round int,
  circuitId int,
  name string,
  date DATE,
  time STRING,
  url STRING
) using csv
options (path "/mnt/tanishqformula1dl/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create constructors table

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
  constructorId int,
  constructorRef string,
  name string,
  nationality string,
  url string
) using json
options(path "/mnt/tanishqformula1dl/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create drivers table

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId int,
  driverRef string,
  number int,
  code string,
  name struct<forename:string,surname:string>,
  dob date,
  nationality string,
  url string
) using json
options(path "/mnt/tanishqformula1dl/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create results table

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grid int,
  position int,
  positionText string,
  positionOrder int,
  points int,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime string,
  fastestLapSpeed float,
  statusId string
)
using json
options(path "/mnt/tanishqformula1dl/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create pit stops table

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId int,
  duration string,
  lap int,
  milliseconds int,
  raceId int,
  stop int,
  time string
) using json
options(path "/mnt/tanishqformula1dl/raw/pit_stops.json",multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create tables for list of files
-- MAGIC #####Create lap_times table

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
) using csv
options(path "/mnt/tanishqformula1dl/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create qualifying table

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  constructorId int,
  driverId int,
  number int,
  position int,
  q1 string,
  q2 string,
  q3 string,
  qualifyId int,
  raceId int
) using json
options(path "/mnt/tanishqformula1dl/raw/qualifying",multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

