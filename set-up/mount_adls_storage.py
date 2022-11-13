# Databricks notebook source
storage_account_name = "formual1dl"
client_id = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

container_name = "processed"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

container_name = "presentation"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

container_name = "demo"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/formual1dl/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formual1dl/processed")

# COMMAND ----------

storage_account_name_temp = "mydbml"
container_name = "ml-datasets"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name_temp}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name_temp}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/mydbml/ml-datasets")

# COMMAND ----------

dbutils.fs.unmount("/mnt/mydbml/ml-datasets")

# COMMAND ----------

