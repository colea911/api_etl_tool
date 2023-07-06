
from helpers.etl_classes import API, TokenBucket
from helpers.transform_functions import transform_getrawmempooldata
from helpers.etl_classes import DataConfigs
from helpers.functions import merge_rawmempooldata, release_rawmempooldata
from helpers.schemas import MempoolEntry



# rawmempooldata CONFIGS

# Define TokenBuckets at API service level
qn_token_bucket = TokenBucket(10, 10)

# Define DataConfigs at SQL table level
configs_rawmempooldata = DataConfigs(       
    id = "mempool",
    orm = MempoolEntry,
    merge_function = merge_rawmempooldata,
    release_function = release_rawmempooldata,
    merge_type = dict
)

# Define all APIs
qn_getrawmempool = API(
    url="https://snowy-thrilling-sun.btc.discover.quiknode.pro/63e62cd654b9638402d07186db32b187f4810b14/",
    payload={"method": "getrawmempool", "params":[True]},
    headers={"Content-Type": "application/json"},
    id="mempool",
    service="quicknode",
    token_bucket=qn_token_bucket,
    transform_function = transform_getrawmempooldata,
    configs = configs_rawmempooldata
)