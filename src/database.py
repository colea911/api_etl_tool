
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os



# Get the absolute path to the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the credentials directory
credentials_dir = os.path.join(current_dir, '..', 'credentials', 'focal-shadow-391317-39771525acf3.json')


# Set up the SQLAlchemy engine
engine = create_engine('bigquery://', credentials_path=credentials_dir)

# Set up the session
Session = sessionmaker(bind=engine)

