"""ETL Orchestrator

__author__ = 'name'


This script orchestrates the extraction of data from variously configured API
endpoints, the loading of that data in an Azure Gen2 data lake, and logs each
step in an Azure SQL database.

This is a configuration-driven ETL pipeline. The configuration of each load is
stored in a csv file in a data lake folder created for the source system.

All sensitive credentials and connection strings are retrieved from Azure 
Key vault.

Custom modules are used to retrieve connection strings and credentials, 
request apis, interact with data lake blob storage, and interact with the 
SQL server.

The general ETL pattern is:

    00 - Initialize: initialize and monitor the overall process of extracting 
                data and loading it
    01 - Main: orchestrate the process and to call the required dependencies
    02 - Monitor Loads: monitor the process of extracting and loading data for 
                each row in the configuration file
    03 - Source to Data Lake: accomplish the work of accessing, extracting, and 
                loading data from the source to the data lake

Azure Service Requirements:

 * Azure Data Lake Gen2 storage (stores config file, load destination)
 * Azure Key vault (stores connection strings, passwords, & api tokens)
 * Azure SQL server (logs batch loads)

Python Module Requirements:

 * requests
 * json
 * xml.etree.ElementTree
 * pandas
 * azure.identity
 * azure.keyvault.secrets
 * azure.storage.blob
 * pyodbc

Custom Module Requirements:

 * connections.py
 * process_logging.py
 * blob_functions.py
 * request_api.py

Syntax: python ./etl_orchestrator.py raw bureauoflaborstatistics
"""

import sys
from datetime import datetime

from process_logging import log_batch, log_batch_step, get_last_batch_run
from blob_functions import blob_read_csv, blob_file_count
import request_api as ra

# Connections module contains code to generate connection strings.
import connections as cn
cn.init()

def main(data_lake_schema_name, data_lake_folder_name):
    # Define the source to be processed.
    source_schema = 'API'
    init_pipeline_name = 'python_test'
    orchestration_tool = 'python'
    do_not_include_today_flag = 0
    config_file = 'api_config.csv'
    date_string = datetime.now().strftime("%Y%m%d")
    date_folder_name = datetime.now().strftime("/%Y/%m/%d")  # ("/%Y/%m")
    root_folder_name = data_lake_schema_name + '/' + data_lake_folder_name
    
    # 00 - Initialize:
    # Retrive the api endpoint configuration.
    config_data_list = blob_read_csv(config_file, root_folder_name)
    full_folder_name = root_folder_name + '/' \
        + config_data_list[0]['endpoint_name']

    # Define the batch logging constant variables.
    project = config_data_list[0]['project']
    source_name = config_data_list[0]['source_name']
    target_system = config_data_list[0]['target_system']
    target_data_source = config_data_list[0]['target_data_source']

    # Begin batch logging.
    need_return_value = True
    batch_id = None
    status = 'Begin'
    return_value = log_batch(
        batch_id, init_pipeline_name, need_return_value, orchestration_tool, 
        project, source_name, status, target_system)
    # Save returned batch logging variables to update audit record at batch 
    # completion.
    batch_id = return_value[0]

    # 01 - Main
    # Loop through each of the endpoints.
    for config in config_data_list:
        target_file_count = None
        step_name = 'Load File ' + config['endpoint_name']

        target_object = config['endpoint_name']
        print('Starting load: ' + source_name + ': ' + target_object)
        target_schema = root_folder_name + '/' + target_object
        full_folder_name = root_folder_name + '/' \
            + config['endpoint_name'] # + date_folder_name
        target_update_strategy = config['target_update_strategy']
        no_schema_folder_name = data_lake_folder_name + '/' \
            + target_object  # + date_folder_name

        # 02 - Monitor Loads
        # Begin batch step logging.
        step_status = 'Begin'
        log_batch_step(
            batch_id, step_name, step_status, source_schema, root_folder_name, 
            target_update_strategy, target_schema, target_object, 
            target_file_count)

        # Get password and/or token keyvault secret names from the config file.
        try:
            password = cn.secret_client.get_secret(
                config['keyvault_secret_password_name']).value
        except ValueError:
            password = 'none'
        if config['keyvault_secret_get_access_token_name'] != 'none':
            try:
                access_token = cn.secret_client.get_secret(
                    config['keyvault_secret_get_access_token_name']
                    ).value
            except ValueError:
                access_token = ''
        else:
            access_token = ''

        aip_config_row = config

        # For future dev
        date_last_run = get_last_batch_run(
            target_system, full_folder_name, target_object, 
            do_not_include_today_flag) 

        # 03 - Source to Data Lake
        # Call the api.
        step = ra.ApiCall(
            aip_config_row, full_folder_name, access_token, password)

        # Count the number of files copied to blob storage for batch step log.
        if step.success_response == 'Success':
            status = step.success_response
            target_file_count = blob_file_count(
                data_lake_schema_name, no_schema_folder_name, target_object 
                + '-' + date_string)
        else:
            status = 'Failure: ' + step.success_response  

        # Complete batch step logging for the current endpoint.
        log_batch_step(
            batch_id, step_name, status, source_schema, root_folder_name, 
            target_update_strategy, target_schema, target_object, 
            target_file_count)
        print('Finished load: ' + source_name + ': ' + target_object)

        if step.success_response == 'Success':
            pass
        else:
            # Complete the batch logging if current step failed.
            status = 'Failure: ' + target_object
            need_return_value = False
            log_batch(
                batch_id, init_pipeline_name, need_return_value, 
                orchestration_tool, project, source_name, status, target_system)
            break

    # Complete batch logging for a successful load.
    if step.success_response == 'Success':
        need_return_value = False
        status = 'Success'
        log_batch(
            batch_id, init_pipeline_name, need_return_value, orchestration_tool, 
            project, source_name, status, target_system)
    else:
        pass
    print(step.success_response)
    
if __name__ == '__main__':
    data_lake_schema_name = sys.argv[1]
    data_lake_folder_name = sys.argv[2]
    main(data_lake_schema_name, data_lake_folder_name)