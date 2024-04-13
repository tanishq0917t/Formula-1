# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.tanishqformula1dl.dfs.core.windows.net",
    "7+y8NZ4b/0Tfh/FnhOVLE1yyZYQOuEmjTSaE+8yNe6RcrtuUArtKJYGI7vEAI60L5xjjSMzuGUFZ+AStDj4AdA=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@tanishqformula1dl.dfs.core.windows.net/circuits.csv"))