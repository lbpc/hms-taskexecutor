import pymysql
import pg8000

__all__ = ["MySQLClient", "PostgreSQLClient"]


class DbApi2Compatible:
    _connection = None

    def __enter__(self):
        self._cursor = self._connection.cursor()
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.commit()
        self._cursor.close()
        self._connection.close()


class MySQLClient(DbApi2Compatible):
    def __init__(self, host, user, password, port=3306, database="mysql"):
        self._connection = pymysql.connect(database=database,
                                           host=host,
                                           port=port,
                                           user=user,
                                           password=password)


class PostgreSQLClient(DbApi2Compatible):
    def __init__(self, host, user, password, port=5432, database=None):
        self._connection = pg8000.connect(database=database,
                                          host=host,
                                          port=port,
                                          user=user,
                                          password=password)
