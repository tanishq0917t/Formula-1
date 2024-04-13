# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.tanishqformula1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.tanishqformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.tanishqformula1dl.dfs.core.windows.net", "sp=rl&st=2024-02-24T05:57:59Z&se=2024-02-24T13:57:59Z&spr=https&sv=2022-11-02&sr=c&sig=hPN5FyMUYYJQxXwNtro8Cv%2F0VhSx7%2FzefMe7GunSlhw%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@tanishqformula1dl.dfs.core.windows.net/circuits.csv"))