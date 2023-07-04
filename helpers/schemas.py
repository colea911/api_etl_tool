
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base 


Base = declarative_base()

# Create the database engine
engine = create_engine('sqlite:///bitpool.db', echo=True)
Session = sessionmaker(bind=engine)


class MempoolEntry(Base):
    __tablename__ = 'mempool_transactions'

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


# Create the tables based on the defined models
Base.metadata.create_all(bind=engine)