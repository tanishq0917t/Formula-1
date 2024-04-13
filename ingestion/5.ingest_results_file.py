# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1 Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("grid",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("positionText",StringType(),True),
                                    StructField("positionOrder",IntegerType(),True),
                                    StructField("points",FloatType(),True),
                                    StructField("laps",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True),
                                    StructField("fastestLap",IntegerType(),True),
                                    StructField("rank",IntegerType(),True),
                                    StructField("fastestLapTime",StringType(),True),
                                    StructField("fastestLapSpeed",FloatType(),True),
                                    StructField("statusId",IntegerType(),True)])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_with_column_df=results_df.withColumnRenamed("resultId","result_id") \
                                 .withColumnRenamed("raceId","race_id") \
                                 .withColumnRenamed("driverId","driver_id") \
                                 .withColumnRenamed("constructorId","constructor_id") \
                                 .withColumnRenamed("positionText","position_text") \
                                 .withColumnRenamed("positionOrder","position_order") \
                                 .withColumnRenamed("fastestLap","fastest_lap") \
                                 .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                 .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                 .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3 Drop the unwanted column

# COMMAND ----------

results_final_df=results_with_column_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4 Write the output to processed container in parquet format

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")