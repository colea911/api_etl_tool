from helpers.logger import logger
import pickle
import os
import queue
class DataStore:
    '''
    Object to persistently store configs & data for Pipeline instance.
    '''

    default_name = "api_datastore"

    def __init__(self, datastore_name = default_name):
        self.logger = logger
        self.datastore_name = datastore_name
        self.apis = []
        self.etl_mappings = {}
        self.load_persistent_storage()

    def load_apis(self, apis):
        '''
        Loads API objects & their respective ETLMapping objects
        '''
        for api in apis:
            self.apis.append(api)
            # ensure ETL configs included
            if api.etl_mapping.id not in self.etl_mappings:
                self.etl_mappings[id] = api.etl_mapping


    def load_persistent_storage(self):
        '''
        Load data from a pickle file if it exists; otherwise, initialize objects afresh.
        '''
        file_path = f"{self.datastore_name}.pickle"
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, 'rb') as file:
                data = pickle.load(file)

                self = data

            self.logger.info(f'DataStore successfully loaded via {self.datastore_name}.pickle')

        else:
            # No persistent storage - instance variables

            self.logger.info(f'No persistent data detected via {self.datastore_name}.pickle')

    def save_persistent_storage(self):
        '''
        Save the entire DataStore class instance to a pickle file.
        '''
        
        self.convert_queues_to_lists()
        file_path = f"{self.datastore_name}.pickle"
        with open(file_path, 'wb') as file:
            pickle.dump(self, file)
            self.logger.info(f'Successfully saved pipeline state at {self.datastore_name}.pickle')




    def convert_lists_to_queues(self):
        '''
        Loads API objects & their respective ETLMapping objects
        '''

        for id, etl_mapping in self.etl_mappings.items():
            my_queue = queue.Queue()
            for item in etl_mapping.queue:
                my_queue.put(item)
            self.etl_mappings[id].queue = my_queue

    def convert_queues_to_lists(self):
        
        for id, etl_mapping in self.etl_mappings.items():
            my_list = []
            queue = etl_mapping.queue 
            while not queue.empty():
                item = queue.get()
                my_list.append(item)
            self.etl_mappings[id].queue  = my_list

    def reload_queues(self):
        for id, etl_mapping in self.etl_mappings.items():
            queue = queue.Queue()
            for api in etl_mapping:
                queue.put(api)
            self.etl_mapping[id] = queue

    def check_functional(self):
        assert len(self.apis) >= 1
        assert len(self.etl_mappings) >= 1