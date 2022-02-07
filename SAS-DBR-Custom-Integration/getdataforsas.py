# Databricks notebook source
import os
import uuid

# COMMAND ----------

#Define input variables for the job
dbutils.widgets.text("query","select * from default.test limit 2500;")
dbutils.widgets.text("tmpfilename","tmpfilename")

# COMMAND ----------

#get values from jobs
query = dbutils.widgets.get("query")
tmpfilename = dbutils.widgets.get("tmpfilename")

# COMMAND ----------

# Constants for the integration

#Path on SAS Server where data should be uploaded
SAS_PATH_FOR_FILE = '/opt/sas/sasdata'
#Path on DBR server where temp extracted file should land
DBR_PATH_FOR_FILE = '/tmp/outputcsv'

#SAS_HOST URL
SAS_HOST = '192.168.1.1'

#SAS Server user to be used in ssh connection
SAS_SSH_USER = 'sas'
#Path to SSH key for the SSH connection, can be changed to use DBR secret
SAS_SSH_KEY_PATH = '/databricks/sasconfig/id_rsa_sas_dbr'

#Get a temp filename for extracted data
dbr_temp_filename = str(uuid.uuid1())

# COMMAND ----------

###CLEAR DATA to be removed
dbutils.fs.rm(DBR_PATH_FOR_FILE,True)

# COMMAND ----------

#run submitted query
data = spark.sql(query)
#export data using single node, can be optimised
data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(DBR_PATH_FOR_FILE+"/"+dbr_temp_filename)

exported_files = dbutils.fs.ls(DBR_PATH_FOR_FILE+"/"+dbr_temp_filename)
print(exported_files)

#out of all exported file get filename with data
for file in exported_files:
  if (str(file.name).endswith(".csv")):
      exported_file = "/dbfs"+DBR_PATH_FOR_FILE+"/"+dbr_temp_filename+"/"+file.name

print (exported_file)


# COMMAND ----------

# archive exported csv file
os.system("gzip "+exported_file)

# scp to target sas server
scp_command = "scp -o \"StrictHostKeyChecking=no\" -i "+SAS_SSH_KEY_PATH+" "+exported_file + ".gz "+SAS_SSH_USER+"@"+SAS_HOST+":" + SAS_PATH_FOR_FILE +"/"+tmpfilename + "  "
os.system(scp_command)
