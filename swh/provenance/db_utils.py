import io
import psycopg2

from configparser import ConfigParser
from pathlib import PosixPath


def config(filename: PosixPath, section: str):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db


def typecast_bytea(value, cur):
    if value is not None:
        data = psycopg2.BINARY(value, cur)
        return data.tobytes()


def adapt_conn(conn):
    """Makes psycopg2 use 'bytes' to decode bytea instead of
    'memoryview', for this connection."""
    t_bytes = psycopg2.extensions.new_type((17,), "bytea", typecast_bytea)
    psycopg2.extensions.register_type(t_bytes, conn)

    t_bytes_array = psycopg2.extensions.new_array_type((1001,), "bytea[]", t_bytes)
    psycopg2.extensions.register_type(t_bytes_array, conn)


def connect(filename: PosixPath, section: str):
    """ Connect to the PostgreSQL database server """
    conn = None

    try:
        # read connection parameters
        params = config(filename, section)

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        adapt_conn(conn)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn


def execute_sql(conn: psycopg2.extensions.connection, filename: PosixPath):
    with io.open(filename) as file:
        cur = conn.cursor()
        cur.execute(file.read())
        cur.close()
        conn.commit()
