
from helpers.token_bucket import TokenBucket
from src.api_class import API
from src.etl_mapping import transform_getrawmempool, merge_getrawmempool, release_getrawmempool, ETLMapping
from src.models import MempoolEntry
from helpers.util_functions import generate_bq_reference
from credentials.quicknode_authorization import quicknode_token


# rawmempooldata CONFIGS

# Define TokenBuckets at API service level
qn_token_bucket = TokenBucket(10, 10)

# Define LoadTransformConfigs at SQL table level
configs_rawmempooldata = ETLMapping(       
    id = "mempool",
    orm = MempoolEntry,
    merge_function = merge_getrawmempool,
    release_function = release_getrawmempool,
    sqlite_table_name = "mempool_transactions",
    bq_table_name = generate_bq_reference('focal-shadow-391317','mempool','mempool_transactions'),
    bq_clustering_column = "tx",
    merge_type = {},
    interval = 60
)


# Define all APIs
qn_getrawmempool = API(
    url="https://snowy-thrilling-sun.btc.discover.quiknode.pro/63e62cd654b9638402d07186db32b187f4810b14/",
    payload={"method": "getrawmempool", "params":[True]},
    headers={"Content-Type": "application/json", "Authorization":quicknode_token},
    service="quicknode",
    token_bucket=qn_token_bucket,
    response_transform_function = transform_getrawmempool,
    etl_mapping = configs_rawmempooldata,
    interval = 30
)