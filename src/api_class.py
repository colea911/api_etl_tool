



from helpers.logger import logger
import json
import time
import requests


# API is for easy ETL configuration at API Level.

class API:
    def __init__(self, url, payload, headers, service, etl_mapping, response_transform_function, interval, token_bucket = None, multiple_params = None):
        self.url = url
        self._payload = payload
        self.headers = headers
        self.service = service
        self._token_bucket = token_bucket
        self._multiple_params = multiple_params
        self._etl_mapping = etl_mapping
        self._responses = []
        self._logger = logger
        self._interval = interval
        self.response_transform_function = response_transform_function

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
    def token_bucket(self):
        return self._token_bucket

    @property
    def multiple_params(self):
        return self._multiple_params
    
    @property 
    def responses(self):
        return self._responses
    
    @property 
    def interval(self):
        return self._interval

    @property 
    def etl_mapping(self):
        return self._etl_mapping

    def is_ready(self):
        if not all([self.url, self._payload, self.headers]):
            raise ValueError("API configuration is not complete")
        
    def single_request(self):
        timestamp = int(time.time()) # record timestamp of call
        self.token_bucket.consume()
        response = requests.post(self.url, headers=self.headers, data=self.payload)
        if response.status_code == 200:
            setattr(response, 'timestamp', timestamp)
            self._responses.append(response)
        else:
            logger.error(f"Error - Got HTTP code {response.status_code}")
            

    def multiple_requests(self):
        for p in self._multiple_params:
            timestamp = int(time.time()) # record timestamp of call
            self.token_bucket.consume()
            response = requests.post(self.url, headers=self.headers, data=self.modified_payload(self._payload, p))
            if response.status_code == 200:
                setattr(response, 'timestamp', timestamp)
                self._responses.append(response)
            else:
                logger.error(f"Error - Got HTTP code {response.status_code}")
       
    def extract(self):
        self.is_ready()
        if self._multiple_params:
            self.multiple_requests()
        else:
            self.single_request()
        if self._responses:
            self.etl_mapping.queue.put(self)