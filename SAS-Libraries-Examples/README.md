# SAS-Libraries-Examples

Few examples on how to define lib statements to connect from SAS to DBR, so Databricks data can be consumed from SAS Code. 
The code contains examples for 
- SAS JDBC
- SAS Interface for Spark 
A code Sample for Integrating Databricks data into SAS Analysis. 
In the code we cover Integrating data using JDBC and SAS/ACCESS to Spark Interface.
There are both pretty familiar in terms of how they need to be setup.
Platform SAS 9.4m5 SAS/ACCESS to Spark Interface or SAS/ACCESS to JDBC is supported by the license 
## Step-by-Step guide:
1. Download jdbc driver from https://databricks.com/spark/jdbc-drivers-download
2. Deploy jdbc driver to SAS server and add path to CLASSPATH variable
3. Obtain jdbc connection url to Databricks, using Advance Option menu on Databricks cluster page (more details are here https://docs.microsoft.com/en-ca/azure/databricks/integrations/bi/jdbc-odbc-bi#jdbc-driver)
4. Create Databricks Access Token for SAS
5. Create SAS Authentification Domain (eg AuthDBR) for integration for set User_id as token, and password with the obtained Databricks token (more details on how set SAS Auth Domanain https://documentation.sas.com/doc/en/bicdc/9.4/bisecag/p1du6ccnyjmlkdn1pwc9q11m088w.htm )
6. Assign the created Authentification Domain to the users who suppose to have access to Databrick data
