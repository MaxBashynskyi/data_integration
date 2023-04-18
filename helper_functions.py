from google.cloud import bigquery


def flatten_json(data, parent_key='', sep='.'):
    #Function that flattens received json with respect to BQ structure
    items = []
    for k, v in data.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for idx, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_json(item, f'{new_key}.{idx}', sep=sep).items())
                else:
                    items.append((f'{new_key}.{idx}', item))
        else:
            items.append((new_key, v))
    return dict(items)


def dict_to_struct(d):
    #Helper function for upsert function to convert items from json file to BQ structures
    if not isinstance(d, dict):
        return d
    return {k: (dict_to_struct(v) if isinstance(v, dict) else (bigquery.Array([dict_to_struct(x) for x in v]) if isinstance(v, list) and v and isinstance(v[0], dict) else v)) for k, v in d.items()}


def python_type_to_bigquery_type(value):
    #Helper function for upsert function to convert to BQ types
    if value is None:
        return "NULLABLE"
    elif isinstance(value, str):
        return "STRING"
    elif isinstance(value, int):
        return "INT64"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, bool):
        return "BOOL"
    else:
        raise TypeError(f"Unsupported type: {type(value)}")


