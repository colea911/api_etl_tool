
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os



# from sqlalchemy import create_engine, Column, Integer, String
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base
from google.cloud import bigquery


# Get the absolute path to the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the credentials directory
credentials_dir = os.path.join(current_dir, '..', 'credentials', 'focal-shadow-391317-39771525acf3.json')
print(credentials_dir)


# Set the project ID and dataset ID
project_id = 'focal-shadow-391317'
dataset_id = 'mempool'
table_id = "mempool_transactions"

# Set up the SQLAlchemy engine
engine = create_engine('bigquery://', credentials_path=credentials_dir)





# Set up the session
Session = sessionmaker(bind=engine)

# Set up the base class for declarative models
Base = declarative_base()


# Example getrawmempooldata RPC call

# {'0de76e865e9822c5d9e0bb612298914b336c7b3d194cf71a8f4fcb399d293c53': {'vsize': 140,
#   'weight': 559,
#   'time': 1688148387,
#   'height': 796595,
#   'descendantcount': 1,
#   'descendantsize': 140,
#   'ancestorcount': 2,
#   'ancestorsize': 1312,
#   'wtxid': '5c665b0db91942832320c1f0075a99a6ed5d4f6c57a32cb27422cc99add10690',
#   'fees': {'base': 1.232e-05,
#    'modified': 1.232e-05,
#    'ancestor': 0.00011752,
#    'descendant': 1.232e-05},
#   'depends': ['03025bb9678c53f064553b5a38a50cdcba70b342fb804c06b91878140b89aba2'],
#   'spentby': [],
#   'bip125-replaceable': True,
#   'unbroadcast': False,
#   'record_time': 1688183816},

 


class MempoolEntry(Base):
    __tablename__ = f"{project_id}.{dataset_id}.{table_id}"

    tx = Column(String, primary_key=True)
    vsize = Column(Integer)
    weight = Column(Integer)
    time = Column(Integer)
    height = Column(Integer)
    descendantcount = Column(Integer)
    descendantsize = Column(Integer)
    ancestorcount = Column(Integer)
    ancestorsize = Column(Integer)
    wtxid = Column(String)
    base_fee = Column(Float)
    modified_fee = Column(Float)
    ancestor_fee = Column(Float)
    descendant_fee = Column(Float)
    bip125_replaceable = Column(Boolean)
    unbroadcast = Column(Boolean)
    record_time = Column(Integer)
    service = Column(String)
    last_seen_mempool = Column(Integer)

    @classmethod
    def create_from_dict(cls, data):
        valid_data = {k: v for k, v in data.items() if hasattr(cls, k)}
        return cls(**valid_data)




# SQL statement to create the table
create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_id}` (
        tx STRING,
        vsize INTEGER,
        weight INTEGER,
        time INTEGER,
        height INTEGER,
        descendantcount INTEGER,
        descendantsize INTEGER,
        ancestorcount INTEGER,
        ancestorsize INTEGER,
        wtxid STRING,
        base_fee FLOAT64,
        modified_fee FLOAT64,
        ancestor_fee FLOAT64,
        descendant_fee FLOAT64,
        bip125_replaceable BOOLEAN,
        unbroadcast BOOLEAN,
        record_time INTEGER,
        service STRING,
        last_seen_mempool INTEGER
    )
"""

# Execute the SQL statement
with engine.connect() as conn:
    conn.execute(create_table_sql)




