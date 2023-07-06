
import concurrent.futures
import time
from apscheduler.schedulers.background import BackgroundScheduler
from queue import Queue
import datetime

# Queue for managing data
api_queue = Queue()

# import APIs we plan to run
from configs.api_configs import qn_getrawmempool

# Import DataStore 
from helpers.etl_classes import DataStore

# Instance DataStore
etl_pipeline = DataStore()



def main():
    # using ThreadPoolExecutor to manage threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # APScheduler for running tasks at intervals
        scheduler = BackgroundScheduler()

        start_date = datetime.datetime.now() + datetime.timedelta(seconds=1)
        
        # scheduling the tasks
        scheduler.add_job(qn_getrawmempool.enqueue_request, 'interval', start_date = start_date, args=[api_queue], seconds=25)  # runs every 10 seconds
        scheduler.add_job(etl_pipeline.clear_pipeline, 'interval', args=[api_queue], seconds=60)  # runs every 15 seconds
        scheduler.start()

        try:
            # Keeps the main thread alive
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            scheduler.shutdown()


if __name__ == "__main__":
    main()
