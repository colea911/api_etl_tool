
import requests
import json
import time


class TokenBucket:
    def __init__(self, capacity, fill_rate):
        self.capacity = capacity  # Maximum number of tokens in the bucket
        self.tokens = capacity  # Current number of tokens in the bucket
        self.fill_rate = fill_rate  # Tokens added per second
        self.last_time = time.time()  # Timestamp of the last token consumption
    
    def consume(self, tokens=1):
        current_time = time.time()
        elapsed_time = current_time - self.last_time

        # Refill the bucket if tokens need to be added
        tokens_to_add = elapsed_time * self.fill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)

        # Check if enough tokens are available
        if tokens <= self.tokens:
            self.tokens -= tokens
            self.last_time = current_time
            return True  # Request is allowed

        # Sleep for the remaining time if rate is exceeded
        time.sleep(1 - (self.tokens / self.fill_rate))
        self.tokens = 0
        self.last_time = time.time()
        return False  # Request is rejected due to rate limit

qn_token_bucket = TokenBucket(10, 10)


apis = {
    # quicknode - getrawmempool
    'qn_getrawmempool': {
        "url":"https://snowy-thrilling-sun.btc.discover.quiknode.pro/63e62cd654b9638402d07186db32b187f4810b14/",
        "payload":{"method": "getrawmempool"},
        "headers":{"Content-Type": "application/json"},
        "api_type":"mempool",
        "service":"quicknode",
        "token_bucket": qn_token_bucket
       },
    # quicknode - something else    
    "qn_estimatesmartfee": {
        "url":"https://snowy-thrilling-sun.btc.discover.quiknode.pro/63e62cd654b9638402d07186db32b187f4810b14/",
        "payload":{"method": "estimatesmartfee"},
        "headers":{"Content-Type": "application/json"},
        "api_type":"fee_estimate",
        "token_bucket":qn_token_bucket  
       },
}


def request_wrapper(api_name, param):
    api = apis[api_name]
    url, headers, base_payload, api_type, token_bucket = api['url'], api['headers'], api['payload'], api['api_type'], api['token_bucket']

    def execute_request():
        payload = base_payload.copy()  # Create a copy of the base payload

        # Modify the payload based on the param if needed
        payload['params'] = [param]

        # Token bucket sleeps if we exceed rate limit
        token_bucket.consume()

        # Make the API request using the modified payload/headers
        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        return(response)

    return execute_request


def flatten_dict(dictionary):
    flattened_dict = {}
    for k, v in dictionary.items():
        if isinstance(v, dict):
            nested_dict = flatten_dict(v)
            for nk, nv in nested_dict.items():
                flattened_dict[f"{k}.{nk}"] = nv
        else:
            flattened_dict[k] = v
    return flattened_dict


