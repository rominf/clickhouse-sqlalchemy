import chdb

from clickhouse_driver.errors import Error as DriverError
from clickhouse_sqlalchemy.drivers.native.connector import Error

# PEP 249 module globals
apilevel = '2.0'
# TODO: Threads may share the module and connections.
threadsafety = 2
# Python extended format codes, e.g. ...WHERE name=%(name)s
paramstyle = 'pyformat'


def connect(*args, **kwargs):
    """
    Make new connection.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    __slots__ = ("_connection")

    def __init__(self, *args, **kwargs):
        database = kwargs.get("database", "default")
        path = kwargs.get("path", None)
        self._connection = chdb.dbapi.connect(path=path)
        if database != "default":
            cursor = self._connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cursor.execute(f"USE {database}")
        super(Connection, self).__init__()

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return self._connection.cursor()
