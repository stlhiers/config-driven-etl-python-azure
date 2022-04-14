"""Credential & Connection retrieval functions

This script is utilized by other scripts in this program to connect to Azure
Key vault to retrieve required credentials and connection strings.

The URLs and resource names in this file assume that multiple environments
are utilized and are identified by 'development', 'testing', 'production', 
etc. It is likely your naming convention is different. Adjust the variables
and URL strings accordingly.

Python Module Requirements:

 * azure.identity
 * azure.keyvault.secrets
"""
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def init():    
    global bi_environment
    bi_environment = 'dev'
    
    global secret_client
    credential = DefaultAzureCredential()
    secret_client = SecretClient(
        vault_url='https://<initial keyvault url segment>' + bi_environment 
        + '<final keyvault url segment>', credential=credential)
    
    global sql_connection_string
    # Get sql connection string to pass to process_logging.py (so environment can 
    # be set here rather than the module)
    sql_kv_secret = secret_client.get_secret(
        '<Keyvault secret name for Azure sql connection string>').value
    driver= '{ODBC Driver 17 for SQL Server}'
    sql_connection_string = 'DRIVER=' + driver + ';' + sql_kv_secret
    global blob_connection_string
    
    # get data lake key to pass to blob_functions.py
    blob_kv_secret = secret_client.get_secret(
        '<Keyvault secret name for blob storage secret>').value
    blob_connection_string = 'DefaultEndpointsProtocol=https; \
        AccountName=<initial blob account name string>' + bi_environment \
        + '<final blob account name string>;AccountKey=' + blob_kv_secret