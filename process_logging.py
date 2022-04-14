"""Batch logging functions

This script is utilized by other scripts in this program to log load
progress and statuses to the Azure SQL server.

Python Module Requirements:

 * functools
 * pyodbc

Custom Module Requirements:

 * connections.py
"""

from functools import wraps # for debugging function
import pyodbc

from blob_functions import blob_write, blob_read_csv, blob_file_count

# connections module contains code to generate connection strings
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

@debug_log
def create_cursor(connection_string):
    cnxn = pyodbc.connect(connection_string)
    cnxn.autocommit = True
    cursor = cnxn.cursor()
    return cursor

# Manage the batch logging process.
@debug_log
def log_batch(
        batch_id, init_pipeline_name, need_return_value, orchestration_tool, 
        project, source_name, status, target_system):
    cursor = create_cursor(cn.sql_connection_string)
    sql_proc_variables = [
        status, orchestration_tool, project, init_pipeline_name, source_name, 
        target_system, batch_id]

    sql = """\
    DECLARE @rv int;
    SET NOCOUNT ON
    EXEC @rv = METADATA.s_BatchLogging ?, ?, ?, ?, ?, ?, ?;
    SELECT @rv AS return_value;
    """
    cursor.execute(sql, sql_proc_variables)
    if need_return_value:
        return_value = cursor.fetchval()
        return [return_value]

# Managethe batch step logging process.
@debug_log
def log_batch_step(
        batch_id, step_name, step_status, source_schema, root_folder_name, 
        target_update_strategy, target_schema, target_object, target_file_count):
    cursor = create_cursor(cn.sql_connection_string)
    sql_proc_variables = [
        batch_id, step_name, step_status, source_schema, root_folder_name, 
        target_update_strategy, target_schema, target_object, target_file_count]

    sql = """\
    DECLARE @rv int;
    SET NOCOUNT ON
    EXEC @rv = METADATA.s_BatchStepLogging ?, ?, ?, ?, ?, ?, ?, ?, ?
    """
    cursor.execute(sql, sql_proc_variables)
    
# Get the date of the latest successful batch run for the endpoint.
@debug_log
def get_last_batch_run(
        target_system, full_folder_name_value, endpoint_name, 
        do_not_include_today_flag):
    cursor = create_cursor(cn.sql_connection_string)
    sql_proc_variables = [
        target_system, full_folder_name_value, endpoint_name, 
        do_not_include_today_flag]

    sql = """\
    DECLARE @rv date;
    SET NOCOUNT ON
    EXEC @rv = [METADATA].[f_GetBatchStepStartDateTimeFromLastSuccessfulBatch] ?, ?, ?, ?;
    SELECT @rv AS return_value;
    """
    cursor.execute(sql, sql_proc_variables)
    return_value = cursor.fetchval()
    return [return_value]