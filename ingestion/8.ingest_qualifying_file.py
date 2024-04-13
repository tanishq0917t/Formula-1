# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest all qualifying files

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1 Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("q1",StringType(),True),
                                    StructField("q2",StringType(),True),
                                    StructField("q3",StringType(),True),])

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiLine",True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                                 .withColumnRenamed("driverId","driver_id") \
                                 .withColumnRenamed("raceId","race_id") \
                                 .withColumnRenamed("constructorId","constructor_id") \
                                 .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3
# MAGIC  Write the output to processed container in parquet format

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")