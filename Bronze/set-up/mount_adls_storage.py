# Databricks notebook source

# use Azure Key Vault to keep the credentials secret
# printing the list of secrets from Secret Scope
dbutils.secrets.list("formula1-scope")

# set-up the variables and replace hard-coded secrets with the dbutils secrets
storage_account_name = "formula1dl"
client_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-secret")

# set-up the config parameters
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# mounting the storage in the container
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# mounting raw conatiner 
mount_adls("raw")

# mounting processed conatiner 
mount_adls("processed")

# mounting presentation conatiner 
mount_adls("presentation")


# check the list of mount points for raw container
dbutils.fs.ls("/mnt/formula1dl/raw")
dbutils.fs.mounts()

# check the list of mount points for processed container
dbutils.fs.ls("/mnt/formula1dl/processed")
dbutils.fs.mounts()
# check the list of mount points for presentation container
dbutils.fs.ls("/mnt/formula1dl/presentation")
dbutils.fs.mounts()


