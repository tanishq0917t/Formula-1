# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Secrets created for Access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

formula1_account_key=dbutils.secrets.get(scope='formula1-scope',key='formula1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.tanishqformula1dl.dfs.core.windows.net",
    formula1_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@tanishqformula1dl.dfs.core.windows.net/circuits.csv"))