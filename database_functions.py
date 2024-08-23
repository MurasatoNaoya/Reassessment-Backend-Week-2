from psycopg2 import connect
from psycopg2.extensions import cursor, connection
from psycopg2.extras import RealDictCursor


def get_connection(dbname, password="postgres") -> connection:
    return connect(
        dbname=dbname,
        host="localhost",
        port=5432,
        password=password,
        cursor_factory=RealDictCursor)


def get_cursor(connection: connection) -> cursor:
    return connection.cursor()
