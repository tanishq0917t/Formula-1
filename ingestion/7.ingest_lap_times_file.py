# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest All lap_times.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1 Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times/*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df=lap_times_df.withColumnRenamed("raceId","race_id") \
                                 .withColumnRenamed("driverId","driver_id") \
                                 .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3
# MAGIC  Write the output to processed container in parquet format

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")