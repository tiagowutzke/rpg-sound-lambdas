from database.query import Query
from database.connection import Connection
from database.persistence import Persistence


def get_database_objects():
    conn = Connection()
    persistence = Persistence(conn)
    query = Query(conn)

    return conn, persistence, query
