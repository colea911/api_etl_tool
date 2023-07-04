
# Create a file handler to record logs to a file
import logging

# Configure root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a file handler to record logs to a file
file_handler = logging.FileHandler('logfile.txt')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Get the root logger and add the file handler
logger = logging.getLogger()
logger.addHandler(file_handler)
