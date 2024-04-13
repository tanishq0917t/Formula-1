# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1 Read the json file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                       StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema=StructType(fields=[
    StructField("driverId",IntegerType(),False),
    StructField("driverRef",StringType(),True),
    StructField("number",IntegerType(),True),
    StructField("code",StringType(),True),
    StructField("name",name_schema),
    StructField("dob",DateType(),True),
    StructField("nationality",StringType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2 Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit,concat

# COMMAND ----------

drivers_with_column_df=drivers_df.withColumnRenamed("driverId","driver_id")\
                                 .withColumnRenamed("driverRef","driver_ref")\
                                 .withColumn("ingestion_date",current_timestamp())\
                                 .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-3 Drop the unwanted columns
# MAGIC 1. url

# COMMAND ----------

drivers_final_df=drivers_with_column_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-4 Write the output to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")