import os
from lookups import ErrorHandling, PreHookSteps, SQLTablesToReplicate, InputTypes, SourceName, ETLStep, DestinationName
from database_handler import execute_query

def retrieve_sql_files(sql_command_directory_path):
   sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]
   sorted_sql_files = sorted(sql_files)
   return sorted_sql_files

def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schema_tables.append(table.split('.')[1])
    return schema_tables

def execute_sql_folder(db_session, sql_command_directory_path, etl_step, target_schema):
    sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]
    sorted_sql_files =  sorted(sql_files)
    if etl_step == ETLStep.PRE_HOOK:
        sorted_sql_files = sorted_sql_files[etl_step.value:ETLStep.HOOK.value]
    elif etl_step == ETLStep.HOOK:
        sorted_sql_files = sorted_sql_files[etl_step.value:]

    for sql_file in sorted_sql_files:
        with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
            sql_query = file.read()
            sql_query = sql_query.replace('target_schema', target_schema.value)
            return_val = execute_query(db_session= db_session, query= sql_query)
            if not return_val == ErrorHandling.NO_ERROR:
                raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(sql_file))