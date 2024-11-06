from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database URL
DATABASE_URL = 'mysql+mysqlconnector://root:S3cur3Passw0rd!@127.0.0.1/ysearch'

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a base class for the models
Base = declarative_base()

# Define the URL model
class URL(Base):
    __tablename__ = 'crawled_urls'
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP, server_default='CURRENT_TIMESTAMP')
    updated_at = Column(TIMESTAMP, server_default='CURRENT_TIMESTAMP', server_onupdate='CURRENT_TIMESTAMP')

# Create the tables in the database
Base.metadata.create_all(engine)

# Create a session factory
Session = sessionmaker(bind=engine)
session = Session()