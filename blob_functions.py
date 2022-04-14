"""Azure Data Lake Gen2 blob functions

This script is utilized by other scripts in this program to interact with
the Azure Data Lake Gen2, including reading & writing files and getting file
counts.

Python Module Requirements:

 * functools
 * io
 * pandas
 * azure.storage.blob

Custom Module Requirements:

 * connections.py
"""

from functools import wraps # for debugging decorator function

import io as io # used to process csv files
import pandas as pd # used to process csv files

from azure.storage.blob import BlobClient, ContentSettings, BlobServiceClient

# Connections module contains code to generate connection strings.
import connections as cn
cn.init()

# Set DEBUG to true to return function arguments and return values
DEBUG = False

# Print the argument names and values and return values for debugging
def debug_log(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        if DEBUG:
            print(">> Called", function.__name__, "\n",
                {**dict(zip(function.__code__.co_varnames, args)), **kwargs})
        result = function(*args, **kwargs)
        if DEBUG:
            print(">>", function.__name__, "return:\n", result)
        return result
    return wrapper

# Writing content to blob storage
@debug_log
def blob_write(content_type, container_name, file_name, body):
    file_content_settings = ContentSettings(content_type=content_type)
    blob_service_client  = BlobServiceClient.from_connection_string(
        cn.blob_connection_string)
    blob_client = blob_service_client.get_blob_client(
        container_name,blob=file_name)
    blob_client.upload_blob(
        body, overwrite=True, content_settings=file_content_settings)    
 
# Reading csv files (config files, etc.)
@debug_log
def blob_read_csv(config_file, root_folder_name):
    blob = BlobClient.from_connection_string(
        conn_str=cn.blob_connection_string, container_name=root_folder_name, 
        blob_name=config_file)
    config = blob.download_blob()
    with io.BytesIO() as buf:
        config.readinto(buf)
        # needed to reset the buffer, otherwise, panda won't read from the start
        buf.seek(0)
        config_data = pd.read_csv(buf)
        config_data_list = config_data.to_dict('records')
    return config_data_list

# Getting file count for blob storage folder
@debug_log
def blob_file_count(
        data_lake_schema_name, no_schema_folder_name_value, file_name_start):
    blob_service_client  = BlobServiceClient.from_connection_string(
        cn.blob_connection_string)    
    container_client = blob_service_client.get_container_client(
        data_lake_schema_name)
    file_count = 0
    for file in container_client.walk_blobs(
            no_schema_folder_name_value + '/' + file_name_start, delimiter='/'):
        file_count += 1
    return file_count