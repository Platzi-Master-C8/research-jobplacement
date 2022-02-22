from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib

#Engine SQLlite
#engine = create_engine('sqlite:///joboffers.db')

#Engine Postgresql in elephantsql
engine = create_engine("postgresql://vmcjrdti:s7-HvO5SzWdZV0d3_FBpo3djT_GJASQX@castor.db.elephantsql.com/vmcjrdti", echo=True)

Session = sessionmaker(bind=engine, autoflush=False)

Base = declarative_base()

#import psycopg2

#params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
#                                "SERVER=192.168.3.66\SQLExpress;"
#                                "DATABASE=DB_NAME;"
#                                "UID=nameUser;"
#                                "PWD=password")

# params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
#                                 "SERVER=192.168.3.150\gendbserver;"
#                                 "DATABASE=DB_NAME;"
#                                 "UID=nameUser;"
#                                 "PWD=password")

#conn = psycopg2.connect("dbname='my_dbname' user='my_user' host='my_host' password='my_password'")


#engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

