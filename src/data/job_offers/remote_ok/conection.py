import yaml
import psycopg2
from sqlalchemy import create_engine


def read_env():
    with open('./remoteok/load/env.yaml') as f:
        config = yaml.safe_load(f)
    return config


def connection():
    try:
        data = read_env()
        conn_string = f'postgresql://{data["user"]}:{data["pass"]}@{data["host"]}/{data["db"]}'
        # conn_string = 'postgres://user:password@host/dbname'
        db = create_engine(conn_string)
        conn = db.connect()
        conn = psycopg2.connect(conn_string
                                )
        conn.autocommit = True
        return conn
    except:
        print('Error en conexion')
        return None


def connection_elephant():
    try:
        data = read_env()
        conn_string = data["conn_string"]
        db = create_engine(conn_string)
        conn = db.connect()
        # conn = psycopg2.connect(conn_string
                                # )
        conn.autocommit = True
        return conn
    except:
        print('Error en conexion')
        return None
