from get_data import get_data
from bq_connect import init_bigquery_client
from Integration_Funcs import get_fields, compare_schema, upsert_update
from helper_functions import flatten_json_bq

data = get_data()
client = init_bigquery_client()

def get_table_schema(client, dataset, table):
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    table  = client.get_table(table_ref)
    shema = table.schema

    return shema


detected_schema = get_fields(data)
table_schema = get_table_schema(client, 'hr_data', 'employees')
#print(detected_schema)
#print(table_schema[1:])
#compare_schema(table_schema, detected_schema)
#check = compare_schema(table_schema, data)
#print(check)
# data_flatten = [flatten_json_bq(d) for d in data]
#
# for k, v in data[0].items():
#     print(k + ': ', v)
#
# for k, v in data_flatten[0].items():
#     print(k + ': ', v)

upsert_update(client, 'hr_data', 'employees', data, 'id')