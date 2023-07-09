
import time
from contextlib import contextmanager
from sqlalchemy.exc import SQLAlchemyError
from helpers.logger import logger
from src.models import generate_bigquery_table_schema
from src.database import Session
import concurrent.futures
import time
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import traceback


class Pipeline:
    
    default_name = "api_data"


    def __init__(self, datastore, database_type="bigquery"):
        # Containers for pipeline
        self.datastore = datastore
        self.set_database_type(database_type)
        self.logger = logger

    def set_database_type(self, database_type):
        """
        Set the database type. Only allows 'bigquery' or 'sqllite' as valid values.
        """
        if database_type not in ["bigquery", "sqllite", "both"]:
            raise ValueError("Invalid database type. Must be 'bigquery' or 'sqllite' or 'both'.")
        self.database_type = database_type   

    def empty_queue_and_merge(self, etl_mapping):
        '''
        This method empties API response queue for processing
        '''
        while not etl_mapping.queue.empty():

            try:
                api = etl_mapping.queue.get()
                merge_function = etl_mapping.merge_function
                for response in api.responses:
                    api_data = api.response_transform_function(response)
                    merged_data = merge_function(etl_mapping.data, api_data, record_time=response.timestamp, service=api.service)
                    etl_mapping.data = merged_data
            except Exception as e:
                print(f"Error occurred: {str(e)}")
                etl_mapping.queue.put(api)  # Put the object back in the queue if an error occurs
                break

    def release_data_for_load(self, etl_mapping):
        '''
        This method executes logic to release data for LOAD 
        '''

        release_function = etl_mapping.release_function 
        etl_mapping.data, etl_mapping.data_to_load = release_function(etl_mapping.data, etl_mapping.data_to_load)
    
    # context manager for SQL session
    @staticmethod
    @contextmanager
    def session_scope():
        """
        Provide a transactional scope around a series of operations.
        """
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

    def load_data(self, etl_mapping):
        '''
        This method loads data via ORM into SQL format
        '''
        with self.session_scope() as session:
            if etl_mapping.data_to_load:
                try:
                    # Create a list of MempoolEntry objects using a list comprehension
                    mempool_entries = [etl_mapping.orm.create_from_dict(data) for data in etl_mapping.data_to_load]

                    # Add all MempoolEntry objects to the session
                    session.add_all(mempool_entries)
                    self.logger.info('Data successfully loaded')
                except Exception as e:
                    self.logger.info(f'Failed to upload to database - error {e}')
                else:
                    # if no errors then empty data_to_load
                    etl_mapping.data_to_load = []

    def transform_and_load(self, etl_mapping):
        '''
        Main function to ETL from API queue
        '''
        self.empty_queue_and_merge(etl_mapping)
        self.release_data_for_load(etl_mapping)
        self.load_data(etl_mapping)

    def check_tables_exist(self, etl_mapping):
        '''
        Ensure that ORM table exists. Create it if it doesn't
        '''
        with self.session_scope() as session:

                
            ddl_string = generate_bigquery_table_schema(etl_mapping.bq_table_name, etl_mapping.orm, etl_mapping.bq_partition_column, etl_mapping.bq_clustering_column)
            session.execute(ddl_string)


    def start_pipeline(self):
        '''
        Entry function to start Pipeline in multithreaded infinite loop
        '''

        # preliminary checks - ensure tables are created
        for etl_mapping in self.datastore.etl_mappings.values():
            self.check_tables_exist(etl_mapping)

        # using ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # APScheduler for running tasks at intervals
            scheduler = BackgroundScheduler()
            
            start_date = datetime.datetime.now() + datetime.timedelta(seconds=1)

            # scheduling the tasks
            for api in self.datastore.apis:
                scheduler.add_job(api.extract, trigger='interval', start_date=start_date, seconds=api.interval) 
            for etl_mapping in self.datastore.etl_mappings.values():
                scheduler.add_job(self.transform_and_load, args=[etl_mapping], trigger='interval', seconds=etl_mapping.interval)

            scheduler.start()

            try:
                # Keeps the main thread alive
                while True:
                    time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                # Not strictly necessary if daemonic mode is enabled but should be done if possible
                scheduler.shutdown()

            finally:
                # Wait for the threads to finish
                executor.shutdown(wait=True)

                # Delay before saving persistent storage
                time.sleep(1)

                # Save data in the event of failure
                self.datastore.save_persistent_storage()

