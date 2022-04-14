# Python Configuration-driven ETL Scripts for Azure Environment
This project is a proof of concept meant to replicate in python the general ETL patterns currently leveraged in Azure Data Factory pipelines (thank you Tim Donovan for the pattern design!). My goal was to create confi- or metadata-driven ETL scripts in python.

This has been a learning experience for me. I'd love feedback and any suggestions. I tried to apply OOP principles to the project, but I'm still learning! Ideas for how an ETL process can implement OOP is especially appreciated.

The scripts in this project work together to copy api responses to the appropriate folders in an Azure Gen2 data lake and to log the progress of the load in the metadata tables in Azure SQL server. The modules can process multiple api configurations (non-paged, paged w/ "next" link returned, paged with a total page count, json, csv). The load configuration details are driven by csv config files, one for each source system.

## Dependencies
In addition to the custom scripts in this repository, these scripts have the following requirements:

### Azure Service Requirements:

 * Azure Data Lake Gen2 storage (stores config file, load destination)
 * Azure Key vault (stores connection strings, passwords, & api tokens)
 * Azure SQL server (logs batch loads)

### Python Module Requirements:

 * requests
 * json
 * xml.etree.ElementTree
 * pandas
 * azure.identity
 * azure.keyvault.secrets
 * azure.storage.blob
 * pyodbc

## etl_orchestrator.py
This is the main script. The other custom scripts handle the interactions with api endpoints and azure resources used for storage, logging, and credentials.

Syntax: python ./etl_orchestrator.py raw bureauoflaborstatistics

## connections.py
This module retrieves the Active Directory credentials and connections to Azure Key vault for connection strings and sensitive credentials.

## process_logging.py
This module writes to the Azure SQL DB to log the progress of the api batch & steps.

## requests_api.py
This module calls apis with a variety of authentication and paging configurations using the requests module. Current configurations include:

* json_token_paged_count
* json_user_pass_paged_next
* json_user_pass_paged_count
* json_user_pass_not_paged
* json_api_key_not_paged
* xml_user_pass_not_paged
  
current auth_types: token, user-pass, pass, api-key

This module will be expanded to include more api configuration types as the need arises.

## blob_functions.py
This module is utilized by other scripts in this program to interact with the Azure Data Lake Gen2, including reading & writing files and getting file counts.

## SQL DDL files
SQL scripts are provided in this project to create the tables, stored procedures, and functions used throughout the ETL load process. Thanks to Tim Donovan for the stored procedure and function designs and permission to include them in this project.

## Azure Data Lake Gen2
The scripts assume the following general folder structure in the data lake:

    .
    └── raw
       ├── source_a 
       │   ├── endpoint_a
       │   ├── endpoint_b
       │   └── api_config.csv
       └── source_b
       │   ├── endpoint_a
       │   └── api_config.csv       
       └── ...
