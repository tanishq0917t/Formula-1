# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Cluster Scoped Principles
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List the files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@tanishqformula1dl.dfs.core.windows.net/circuits.csv"))