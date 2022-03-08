# SqlAlchemy
from sqlalchemy import create_engine

# Models
from models import Base

DATABASE_URI = ''


def connect_to_db():
    """
    Connect to the database and return the connection
    """
    return create_engine(DATABASE_URI, echo=False)


def create_tables(connection_db):
    """
    Create all tables in the database using the metadata object

    Args:
        connection_db: connection to the database
    """
    Base.metadata.create_all(connection_db)
    print('Tables created')
