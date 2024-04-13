# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1 Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

pit_stop_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("stop",StringType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("duration",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pit_stop_df=spark.read.schema(pit_stop_schema).option("multiLine",True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stop_final_df=pit_stop_df.withColumnRenamed("raceId","race_id") \
                                 .withColumnRenamed("driverId","driver_id") \
                                 .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3
# MAGIC  Write the output to processed container in parquet format

# COMMAND ----------

pit_stop_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")