/******************************************************************


*** A code Sample for Integrating Databricks data into SAS Analysis. 
*** In the code we cover Integrating data using JDBC and SAS/ACCESS to Spark Interface.
*** There are both pretty familiar in terms of how they need to be setup.
*** Platform SAS 9.4m5 SAS/ACCESS to Spark Interface, SAS/ACCESS to JDBC or SAS/ACCESS to ODBC is supported by the license 

   Step-by-Step guide:
	1 - Download jdbc driver from https://databricks.com/spark/jdbc-drivers-download
	2 - Deploy jdbc driver to SAS server and add path to CLASSPATH variable
	3 - Obtain connection details to Databricks, using Advance Option menu on Databricks cluster page 
	(more details are here https://docs.microsoft.com/en-ca/azure/databricks/integrations/bi/jdbc-odbc-bi#jdbc-driver)
	4 - Create Databricks Access Token for SAS
	5 - Create SAS Authentification Domain (eg AuthDBR) for integration for set User_id as token, and password with the obtained Databricks token 
	(more details on how set SAS Auth Domanain https://documentation.sas.com/doc/en/bicdc/9.4/bisecag/p1du6ccnyjmlkdn1pwc9q11m088w.htm )
	6 - Assign the created Authentification Domain to the users who suppose to have access to Databrick data


******************************************************************/


/* Spark Interface
   Update path so it points to the jdbc driver directory. Required only for Spark Interface*/

option set=SAS_ACCESS_CLASSPATH="/opt/sas/drivers";


/*
  Update url with value from Step 3, update authdomain with the name of domain from step 5
  Note: This configuration points only to default database from Databricks, to point to other database update schema value 	
*/
libname dbrspark spark driver="com.simba.spark.jdbc.Driver"
url="jdbc:spark://adb-xxxxxxxxxxxxx.azuredatabricks.net:443/default;transportMode=http;
ssl=1;httpPath=sql/protocolv1/o/xxxxxxxxxxxxx/1019-xxxxxxxxx-tjwinew;AuthMech=3;
UID=token;" authdomain="AuthDBR" schema="default"; 

/*
  JDBC Interface

  Update url with value from Step 3, update authdomain with the name of domain from step 5
  Note: This configuration points only to default database from Databricks, to point to other database update schema value 	 	
*/
libname dbrjdbc jdbc driverclass="com.simba.spark.jdbc.Driver"
url="jdbc:spark://adb-xxxxxxxxxxxxx.azuredatabricks.net:443/default;transportMode=http;
ssl=1;httpPath=sql/protocolv1/o/xxxxxxxxxxxxx/1019-xxxxxxxxx-tjwinew;AuthMech=3;
UID=token;" authdomain="AuthDBR" schema="default";

/*
  ODBC Interface

  Update driver (with path to odbc driver). Host,HTTPPath fields update with value from Step 3, update authdomain with the name of domain from step 5. 
  The other option to use DSN from
  Note: This configuration points only to default database from Databricks, to point to other database update schema value 	 	

*/
libname dbrodbc odbc 
noprompt='Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so;
Host=adb-xxxxxxxxxxxxx.1.azuredatabricks.net;Port=443;
HTTPPath=sql/protocolv1/o/xxxxxxxxxxxxxxxnew;
ThriftTransport=2;SSL=1;AuthMech=3;' authdomain="AuthDBR" schema=default;
