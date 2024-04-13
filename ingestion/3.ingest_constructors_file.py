# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1 Read the json file using the spark dataframe reader

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df=spark.read\
    .schema(constructors_schema)\
    .json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2 Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3 Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df=constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
                        .withColumnRenamed("constructorRef","constructor_ref")\
                        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-4 Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")