
import concurrent.futures
import time
from apscheduler.schedulers.background import BackgroundScheduler
from queue import Queue
import json
from contextlib import contextmanager
from sqlalchemy.exc import SQLAlchemyError
import logging
from helpers.constants import apis, request_wrapper
from helpers.schemas import MempoolEntry, Session
from helpers.logger import logger

# Queue for managing data
transform_queue = Queue()
load_queue = Queue()



# define your SQLAlchemy ORM models here
# https://docs.sqlalchemy.org/en/14/orm/tutorial.html#create-a-schema



def extract_data(api, params = [True]):
    logger.info(f"Extracting: {api}")

    # Run appropriate API call
    
    for p in params:
        execute_request = request_wrapper(api, p)
        timestamp = int(time.time())
        service = apis[api]['service']
        response = execute_request()
    
        # Check if the response has an HTTP status code of 200
        if response.status_code == 200:
            # Add the response to the transform_queue
            response_data = (apis[api]['api_type'], service, response, timestamp)
            transform_queue.put(response_data)
        else:
            print("error")
            print(response)
            print(response.status_code)
        

mempool_txs = {}
fee_estimates = {"quicknode":[]}

def transform_data():
    logger.info(f"Transforming {transform_queue.qsize()} in queue")
    global mempool_txs
    global fee_estimates
    
    



    def update_fee_recommendations(response, service, d, timestamp):
        new_d = json.loads(response.text)['result']
        fee_estimates[service].append(new_d)
    
    def update_mempool_txs(response, service, d, timestamp):

        new_d = json.loads(response.text)['result']

        is_rbc_with_higher_fee = lambda tx: d[tx]['bip125-replaceable'] and new_d[tx]['fees']['modified'] > d[tx]['fees']['modified']
    
        is_earlier_node_time = lambda tx: not d[tx]['bip125-replaceable'] and new_d[tx]['time'] < d[tx]['time']

        for tx in new_d:
            if tx not in d:
                d[tx] = new_d[tx]
                d[tx]['record_time'] = timestamp
                d[tx]['service'] = service
                d[tx]['tx'] = tx
            elif is_rbc_with_higher_fee(tx) or is_earlier_node_time(tx):                 

                d[tx].update(new_d[tx])
                d[tx]['service'] = service
            d[tx]['last_seen_mempool'] = timestamp
        return d
    


    def flatten_dict(d):
        flattened_dict = {}
        for k, v in d.items():
            if isinstance(v, dict):
                nested_dict = flatten_dict(v)
                for nk, nv in nested_dict.items():
                    flattened_dict[f"{k}.{nk}"] = nv
            else:
                flattened_dict[k] = v
        return flattened_dict



    def unload_mempool_txs(api_type, service, d):
        data_to_sql = []
        to_remove = set()
        timestamp = int(time.time())

        for tx, v in d.items():  # Iterate over keys and values
            if v['last_seen_mempool'] < timestamp - 60 * 2:
                v = flatten_dict(v)
                data_to_sql.append(v)
                to_remove.add(tx)

        if data_to_sql:
            load_queue.put((api_type, service, data_to_sql))
            for tx in to_remove:
                d.pop(tx)

        return d

    
    while not transform_queue.empty():
        api_type, service, response, timestamp = transform_queue.get()
        

        if api_type == "mempool":
            mempool_txs = update_mempool_txs(response, service, mempool_txs, timestamp)
            mempool_txs = unload_mempool_txs(api_type, service, mempool_txs)
        elif api_type == "recommended_fee":
            fee_recommendations = update_fee_recommendations(response, service, fee_recommendations, timestamp)


# This decorator helps manage SQL connections to safely connect, disconnect, and rollback
@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Error occurred during session: {str(e)}")
        raise e
    finally:
        session.close()
            

def load_data():
    print(f"initiating load: total {load_queue.qsize()} in queue")

    
    with session_scope() as session:
        while not load_queue.empty():
            api_type, service, data_to_sql = load_queue.get()
        
            if api_type == "mempool":
                try:
                    # Create a list of MempoolEntry objects using a list comprehension
                    mempool_entries = [MempoolEntry.create_from_dict(data) for data in data_to_sql]

                    # Add all MempoolEntry objects to the session
                    session.add_all(mempool_entries)
                    print("success")
                except Exception as e:
                    print("Failed to upload to the database", e)

import datetime
def main():
    # using ThreadPoolExecutor to manage threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # APScheduler for running tasks at intervals
        scheduler = BackgroundScheduler()

        
        
        # scheduling the tasks
        scheduler.add_job(extract_data, 'interval', start_date = datetime.datetime.now() + datetime.timedelta(seconds=1), args=['qn_getrawmempool', [True]], seconds=120)  # runs every 10 seconds
        scheduler.add_job(transform_data, 'interval', seconds=45)  # runs every 15 seconds
        scheduler.add_job(load_data, 'interval', seconds=60)  # runs every 15 seconds

        scheduler.start()

        try:
            # Keeps the main thread alive
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            scheduler.shutdown()


if __name__ == "__main__":
    main()
