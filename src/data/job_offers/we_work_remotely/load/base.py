# SqlAlchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine("postgresql://vmcjrdti:s7-HvO5SzWdZV0d3_FBpo3djT_GJASQX@castor.db.elephantsql.com/vmcjrdti",
                       echo=True)

Session = sessionmaker(bind=engine, autoflush=False)

Base = declarative_base()
