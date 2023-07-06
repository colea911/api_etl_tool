
import requests
import time
from contextlib import contextmanager
import json
from sqlalchemy.exc import SQLAlchemyError
from helpers.logger import logger
from helpers.schemas import Session


# Token bucket is for rate limiting. It is an arg for API class.

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


# API is for easy ETL configuration at API Level.

class API:
    def __init__(self, **properties):
        self._url = properties.get("url")
        self._payload = properties.get("payload")
        self._headers = properties.get("headers")
        self._api_type = properties.get("api_type")
        self._service = properties.get("service")
        self._token_bucket = properties.get("token_bucket")
        self._multiple_params = properties.get("multiple_params")
        self._configs = properties.get("configs")
        self.transform = properties.get("transform_function")
        self._responses = []
        self.logger = logger

    @property
    def url(self):
        return self._url

    @property
    def payload(self):
        if isinstance(self._payload, dict):
            return json.dumps(self._payload)
        return self._payload
    
    def modified_payload(self, entry):
        if isinstance(self._payload, dict):
            self._payload.update(entry) 
            return json.dumps(self._payload)
        return self._payload

    @property
    def headers(self):
        # if isinstance(self._headers, dict):
        #     return json.dumps(self._headers)
        return self._headers

    @property
    def api_type(self):
        return self._api_type

    @property
    def service(self):
        return self._service

    @property
    def token_bucket(self):
        return self._token_bucket

    @property
    def multiple_params(self):
        return self._multiple_params
    
    @property 
    def responses(self):
        return self._responses

    @property 
    def configs(self):
        return self._configs

    def is_ready(self):
        if not all([self._url, self._payload, self._headers]):
            raise ValueError("API configuration is not complete")
        
    def single_request(self):
        timestamp = int(time.time()) # record timestamp of call
        self.token_bucket.consume()
        response = requests.post(self._url, headers=self.headers, data=self.payload)
        if response.status_code == 200:
            setattr(response, 'timestamp', timestamp)
            self._responses.append(response)
        else:
            print("Error - non 200 code")
            

    def multiple_requests(self):
        for p in self._multiple_params:
            timestamp = int(time.time()) # record timestamp of call
            self.token_bucket.consume()
            response = requests.post(self._url, headers=self._headers, data=self.modified_payload(self._payload, p))
            if response.status_code == 200:
                setattr(response, 'timestamp', timestamp)
                self._responses.append(response)
            else:
                print("Error - non 200 code")
       
    def enqueue_request(self, queue):
        self.is_ready()
        if self._multiple_params:
            self.multiple_requests()
        else:
            self.single_request()
        if self._responses:
            queue.put(self)
        

# DataConfigs controls how data flows through ETL process. Each object corresponds to a SQL table.

class DataConfigs:

    def __init__(self, id, orm, merge_function, release_function, merge_type = list):
        self.id = id
        self.orm = orm
        self.merge_function = merge_function
        self.release_function = release_function
        self.merge_type = merge_type


# context manager for SQL session
@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error occurred during session: {str(e)}")
        raise e
    finally:
        session.close()


# DataStore holds the data for ETL. It attempts to make data persistent and holds config settings.

class DataStore:
    

    def load_persistent_storage(self):
        pass

    def __init__(self):
        self.data = {}
        self.data_to_load = {}
        self.loaded_configs = set()
        self.merge_functions = {}
        self.release_functions = {}
        self.orm_schemas = {}
        self.logger = logger

    def absorb_configs(self, configs):
        id = configs.id
        if id not in self.loaded_configs:
            self.merge_functions[id] = configs.merge_function
            self.release_functions[id] = configs.release_function 
            self.orm_schemas[id] = configs.orm  
            self.data[id] = configs.merge_type()
            self.data_to_load[id] = []
            self.data[id] = {}

    def merge_api(self, api):
        self.absorb_configs(api.configs)
        id = api.configs.id
        merge = self.merge_functions[id]
        
        for response in api.responses:
            api_data = api.transform(response)


            merged_data = merge(self.data[id], api_data, record_time=response.timestamp, service=api.service)
            self.data[id] = merged_data
    def empty_queue(self, queue):
        while not queue.empty():
            api = queue.get()
            self.merge_api(api)

    def release_data(self):
        for k,function in self.release_functions.items():
            self.data[k], self.data_to_load[k] = function(self.data[k], self.data_to_load[k].copy())

    def load_data(self):

        with session_scope() as session:
            for k, orm in self.orm_schemas.items():
                if self.data_to_load[k]:
                    try:
                        # Create a list of MempoolEntry objects using a list comprehension
                        mempool_entries = [orm.create_from_dict(data) for data in self.data_to_load[k]]

                        # Add all MempoolEntry objects to the session
                        session.add_all(mempool_entries)
                        self.logger.info('Data successfully loaded')
                    except Exception as e:
                        self.logger.info(f'Failed to upload to database - error {e}')


    def clear_pipeline(self, queue):
        self.empty_queue(queue)
        self.release_data()
        self.load_data()

