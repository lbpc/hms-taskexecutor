import pymysql


class MySQLClient:
    def __init__(self, host, user, password, database="mysql"):
        self._connection = pymysql.connect(database=database,
                                           host=host,
                                           user=user,
                                           password=password)

    def __enter__(self):
        self._cursor = self._connection.cursor()
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.commit()
        self._cursor.close()
        self._connection.close()
