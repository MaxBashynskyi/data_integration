from typing import Dict, List, Union
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.bigquery import SchemaField
from datetime import datetime
from helper_functions import flatten_json, dict_to_struct, python_type_to_bigquery_type


def transform_type(type):
    #Helper function to match types with BQ for get_fields function
    if type == 'str':
        return 'STRING'
    elif type == 'int':
        return 'INTEGER'
    elif type == 'float':
        return 'FLOAT'
    elif type == 'bool':
        return 'BOOLEAN'
    else:
        return type


def get_fields(data: Union[Dict, List]) -> List[SchemaField]:
    #Function to generate BQ table schema from received json file
    fields = []
    if isinstance(data, dict):
        for key, value in data.items():
            if value is None:
                fields.append(SchemaField(key, 'STRING'))
            elif isinstance(value, dict):
                sub_fields = get_fields(value)
                fields.append(SchemaField(key, 'RECORD', fields=sub_fields))
            elif isinstance(value, list):
                if len(value) > 0:
                    if isinstance(value[0], dict):
                        sub_fields = get_fields(value[0])
                        fields.append(SchemaField(key, 'RECORD', fields=sub_fields))
                    else:
                        fields.append(SchemaField(key, transform_type(type(value[0]).__name__), mode='REPEATED'))
                else:
                    fields.append(SchemaField(key, 'STRING', mode='REPEATED'))
            else:
                fields.append(SchemaField(key, transform_type(type(value).__name__)))
    elif isinstance(data, list):
        if len(data) > 0:
            if isinstance(data[0], dict):
                fields = get_fields(data[0])
            else:
                fields = [SchemaField('value', transform_type(type(data[0]).__name__), mode='REPEATED')]
        else:
            fields = [SchemaField('value', 'STRING', mode='REPEATED')]
    return fields


def create_table(client, dataset, table, data):
    #Function to create table in BQ based on received json file is table not exist + add Load Date field
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    schema = get_fields(data)
    schema.insert(0, SchemaField('LoadDate', 'DATE', 'NULLABLE'))

    if schema is None:
        print('Error: Unable to get schema from data')
        return

    table_schema = schema

    table = bigquery.Table(table_ref, schema=table_schema)
    table = client.create_table(table)

    print(f'Table {table.project}.{table.dataset_id}.{table.table_id} created')


def compare_schema(client, dataset, table, data):
    #Function that compares schema in received json file and BQ table
    schema_mismatch = []
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    table = client.get_table(table_ref)
    table_shema = table.schema
    data_schema = get_fields(data)
    
    table_schema_dict = {field.name: field for field in table_schema}
    input_schema_dict = {field.name: field for field in data_schema}
    
    for field, schema in input_schema_dict.items():
        if field not in table_schema_dict:
            schema_mismatch.append(field + ' - not found in table schema')
        else:
            table_field = table_schema_dict[field]
            if schema.field_type != table_field.field_type:
                schema_mismatch.append(field, 'table type: ' + table_field.field_type, 
                                       'input type: ' + field.field_type, '- field type mismatch')
            if schema.mode != table_field.mode:
                schema_mismatch.append(field, 'table type: ' + table_field.mode,
                                       'input type: ' + field.mode, '- field type mismatch')
                
    return schema_mismatch


def update_table_schema(client, dataset, table, data):
    #Function that updates table schema in BQ if received json file has updated structure
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    table = client.get_table(table_ref)

    current_schema = table.schema
    data_schema = get_fields(data)

    current_schema_dict = {field.name: field for field in current_schema}
    data_schema_dict = {field.name: field for field in data_schema}

    schema_updated = False
    for field_name, field in data_schema_dict.items():
        if field_name not in current_schema_dict:
            current_schema.append(field)
            schema_updated = True

    if schema_updated:
        table.schema = current_schema
        client.update_table(table, ['schema'])
        print(f'Table {table.project}.{table.dataset_id}.{table.table_id} schema updated')
    else:
        print(f'No new fields found in JSON data, table schema remains unchanged')


def upsert_update(client, dataset, table, data, primary_key):
    #Function to update values for ids which have new values in json file and append new ids from json file
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    table = client.get_table(table_ref)

    # Flatten the JSON data
    flattened_data = [flatten_json(d) for d in data]

    # Transform each nested dictionary into a STRUCT representation
    struct_data = [dict_to_struct(item) for item in flattened_data]

    # Update existing records and insert new records
    for item in struct_data:
        # Create a list of query parameters for each key-value pair in the item
        query_params = [bigquery.ScalarQueryParameter(f"new_{key}", python_type_to_bigquery_type(value), value) for key, value in item.items() if value is not None]

        # Prepare SQL query
        sql = f"""
            MERGE `{dataset_ref}.{table_ref}` T
            USING (SELECT * FROM UNNEST([{','.join([f"IFNULL(@new_{key}, NULL) AS {key}" for key in item.keys()])}])) S
            ON T.{primary_key} = S.{primary_key}
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f"{key} = S.{key}" for key in item.keys() if key != primary_key])}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(item.keys())}) VALUES ({', '.join([f"S.{key}" for key in item.keys()])})
            """

        # Execute the query
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_job = client.query(sql, job_config=job_config)
        query_job.result()





def snapshot_update(client, dataset, table, data):
    pass


def increment_update(client, dataset, table, data):
    pass
