

# import APIs we plan to run
from configs.api_configs import qn_getrawmempool
# Import Pipeline 
from src.pipeline import Pipeline
from src.datastore import DataStore
import os
print(os.environ.get('PYTHONPATH'))
exit
import sys
print(sys.path)
def main():

    # Instance DataStore
    etl_datastore = DataStore("etl_datastore")

    # Load APIs
    etl_datastore.load_apis([qn_getrawmempool])

    # Instance Pipeline
    etl_pipeline = Pipeline(etl_datastore)

    # Start the pipeline
    etl_pipeline.start_pipeline()

if __name__ == "__main__":
    main()
