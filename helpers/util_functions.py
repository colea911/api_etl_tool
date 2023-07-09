import time

def generate_bq_reference(project, dataset, table):
    return ".".join([project, dataset, table])

def flatten_dict(dictionary, prefix=''):
    flattened_dict = {}
    for key, value in dictionary.items():
        new_key = f"{prefix}_{key}" if prefix else key
        if isinstance(value, dict):
            flattened_dict.update(flatten_dict(value, new_key))
        else:
            flattened_dict[new_key] = value
    return flattened_dict

def convert_keys(dictionary):
    converted_dict = {}
    for key, value in dictionary.items():
        new_key = key.replace("-", "_")
        converted_dict[new_key] = value
    return converted_dict





