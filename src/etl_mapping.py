
import json
import time
from helpers.util_functions import convert_keys, flatten_dict
from queue import Queue

#Example getrawmempooldata RPC call

# {'0de76e865e9822c5d9e0bb612298914b336c7b3d194cf71a8f4fcb399d293c53': {'vsize': 140,
#   'weight': 559,
#   'time': 1688148387,
#   'height': 796595,
#   'descendantcount': 1,
#   'descendantsize': 140,
#   'ancestorcount': 2,
#   'ancestorsize': 1312,
#   'wtxid': '5c665b0db91942832320c1f0075a99a6ed5d4f6c57a32cb27422cc99add10690',
#   'fees': {'base': 1.232e-05,
#    'modified': 1.232e-05,
#    'ancestor': 0.00011752,
#    'descendant': 1.232e-05},
#   'depends': ['03025bb9678c53f064553b5a38a50cdcba70b342fb804c06b91878140b89aba2'],
#   'spentby': [],
#   'bip125-replaceable': True,
#   'unbroadcast': False,

# LoadTransformConfigs controls how data flows through ETL process. We might have 1 to Many relationship of LoadTransformConfigs to APIs.

class ETLMapping:

    def __init__(self, id, orm, merge_function, release_function, interval, bq_table_name = None, sqlite_table_name = None, bq_partition_column = None, bq_clustering_column = None, merge_type = {}):
        self.id = id
        self.orm = orm
        self.merge_function = merge_function
        self.release_function = release_function
        self.sqlite_table_name = sqlite_table_name
        self.bq_table_name = bq_table_name
        self.bq_partition_column = bq_partition_column
        self.bq_clustering_column = bq_clustering_column
        self.data_to_load = []
        self.data = merge_type
        self.interval = interval
        self.queue = Queue()


def transform_getrawmempool(response):
    d = json.loads(response.text)['result']    
    return(d)

def merge_getrawmempool(data, api_data, **kwargs):
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

def release_getrawmempool(data, data_to_load):
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
