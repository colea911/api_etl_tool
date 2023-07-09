import logging
from logging.handlers import TimedRotatingFileHandler

# Configure root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Detailed Log
file_handler = TimedRotatingFileHandler('logfile.txt', when='midnight', interval=7, backupCount=14)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Error Log
error_handler = TimedRotatingFileHandler('error.log', when='midnight', interval=1, backupCount=90)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))


# Get the root logger and add both file handlers
logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(error_handler)
