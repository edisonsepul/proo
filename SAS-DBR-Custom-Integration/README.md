# T1A SAS DBR Integration

## About
This project sample shows how can a custom Integration between SAS 9.4 and Databricks be set up. The described method allows consuming Databricks data from SAS Code. This Integration shows better performance compare to common integration methods (SAS JDBC,SAS Interface to Spark, SAS ODBC and over saspy library) and doesn't require any license changes. 
More details about other methods are available here: https://databricks.com/

**Important** This project is not a production version it's just an example and shouldn't be used in any production environments without proper adjustments.

## How to setup
The integration consists of two parts:
1. A DBR notebook that should be deployed as a job on Databricks environment. The notebook extracts data from DBR database as a CSV file, archive the file and uploads it to SAS Server over SSH.
2. A SAS Macro that calls the notebook over API, waits till it finishes and upload data to the defined library/table

### Before start

1. Setup network connection between DBR environment and SAS environment (it can be done over VPN Peering)
2. Create a user on SAS Server with ssh key that's going to be used for integration purposes
3. In Databricks create a system user that is going to be used for API integration, create an API key for the user

### Setup Integration

1. Deploy notebook (getdataforsas.py) to you Databricks environment
2. In the notebook provide correct values for variables 
    - SAS_PATH_FOR_FILE
    - DBR_PATH_FOR_FILE
    - SAS_HOST
    - SAS_SSH_USER
    - SAS_SSH_KEY_PATH
3. Deploy the notebook as a DBR Job and save job_id for future purposes
4. In the provided SAS Code (sasdbr.sas) update values for variables:
    - tmpdirectory
    - access_token
    - dbr_url
    - job_id
    - max_attemps_wait (if needed)
    - attemps_timeout (if needed)
5. Call SAS Macro getdatafromdatabricks using provided example\


