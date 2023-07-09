
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
from helpers.util_functions import generate_bq_reference

# Set up the base class for declarative models
Base = declarative_base()

#Example getrawmempooldata RPC call

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


# All ORMs inherit ParentClass
class ParentClass(Base):
    __abstract__ = True

    @classmethod
    def create_from_dict(cls, data):
        valid_data = {}
        for k, v in data.items():
            if hasattr(cls, k):
                valid_data[k] = v
            else:
                print(f"{k} not in schema")

        return cls(**valid_data)



table_id = generate_bq_reference("focal-shadow-391317", "mempool", "mempool_transactions")



# ORM for getrawmempooldata API
class MempoolEntry(ParentClass):
    __tablename__ = f"{table_id}"

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
    fees_base = Column(Float)
    fees_modified = Column(Float)
    fees_ancestor = Column(Float)
    fees_descendant = Column(Float)
    bip125_replaceable = Column(Boolean)
    unbroadcast = Column(Boolean)
    record_time = Column(Integer)
    service = Column(String)
    last_seen_mempool = Column(Integer)


# Function creates a SQL DDL Create Statement from ORM (note SQLalchemy doesn't support BQ SQL variant)
def generate_bigquery_table_schema(table_name, model, partition_columns, clustering_columns):
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
    inspector = inspect(model)
    columns = inspector.columns

    schema = []
    for column in columns:
        column_name = column.name
        column_type = column.type
        if str(column_type).upper() == "FLOAT":
            column_type = "FLOAT64"
        if str(column_type).upper() == "VARCHAR":
            column_type = "STRING"
        create_table_sql += f"{column_name} {column_type},\n" 
    
    create_table_sql = create_table_sql[:-2] 
    create_table_sql += "\n)\n"
    if partition_columns:
        create_table_sql += f"PARTITION BY {partition_columns}\n"
    if clustering_columns:
        create_table_sql += f"CLUSTER BY {clustering_columns}"   
        
    return create_table_sql

