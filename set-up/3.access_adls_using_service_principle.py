# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principle
# MAGIC 1. Register Azure AD Application/Service Principle
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set spark configuration with App/client id, directory & secret
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = "82503fe9-b4f8-4d73-b6f7-3101d495445c"
tenant_id = "b4052bb2-687b-402c-8d62-0316a99979ca"
client_secret = "Ffz8Q~QSxLr2Jnw~HJFWu9A~i3DmxK1ImMmZ0a7u"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.tanishqformula1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.tanishqformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.tanishqformula1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.tanishqformula1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.tanishqformula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@tanishqformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@tanishqformula1dl.dfs.core.windows.net/circuits.csv"))