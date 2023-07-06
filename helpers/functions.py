


import time


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


def merge_rawmempooldata(data, api_data, **kwargs):

    is_rbc_with_higher_fee = lambda tx: data[tx]['bip125-replaceable'] and api_data[tx]['fees']['modified'] > data[tx]['fees']['modified']

    is_earlier_node_time = lambda tx: not data[tx]['bip125-replaceable'] and api_data[tx]['time'] < data[tx]['time']

    record_time = kwargs['record_time']
    service = kwargs['service']

    for tx in api_data:
        if not isinstance(api_data[tx], dict):
            continue
        if tx not in data:
            data[tx] = api_data[tx]
            data[tx]['record_time'] = record_time
            data[tx]['service'] = service
            data[tx]['tx'] = tx
        elif is_rbc_with_higher_fee(tx) or is_earlier_node_time(tx):                 
            data[tx].update(api_data[tx])
            data[tx]['service'] = service
        data[tx]['last_seen_mempool'] = record_time

    return data

def release_rawmempooldata(data, data_to_load):

    to_remove = set()
    timestamp = int(time.time())
    for tx, tx_data in data.items():  # Iterate over keys and values
        if tx_data['last_seen_mempool'] < timestamp - 60 * 2:
            normalized_data = convert_keys(flatten_dict(tx_data))
            data_to_load.append(normalized_data)
            to_remove.add(tx)

    if data_to_load:
        for tx in to_remove:
            data.pop(tx)

    return data, data_to_load


