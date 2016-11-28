import abc
import pymysql
import pg8000

from taskexecutor.logger import LOGGER

__all__ = ["MySQLClient", "PostgreSQLClient"]


class DBClient(metaclass=abc.ABCMeta):
    def __init__(self, host, user, password, port, database):
        self._host = host
        self._user = user
        self._password = password
        self._port = port
        self._database = database
        self._connection = None
        self._cursor = None

    @abc.abstractmethod
    def execute_query(self, query, values):
        pass


class MySQLClient(DBClient):
    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self._connection = pymysql.connect(database=self._database,
                                           host=self._host,
                                           port=self._port,
                                           user=self._user,
                                           password=self._password,
                                           autocommit=True)
        self._cursor = self._connection.cursor()

    def execute_query(self, query, values):
        LOGGER.info("Executing query: '{}'".format(query % values))
        self._connection.ping(reconnect=True)
        self._cursor.execute(query, values)
        return self._cursor.fetchall()


class PostgreSQLClient(DBClient):
    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self._connect()

    def _connect(self):
        self._connection = pg8000.connect(database=self._database,
                                          host=self._host,
                                          port=self._port,
                                          user=self._user,
                                          password=self._password)
        self._cursor = self._connection.cursor()

    def execute_query(self, query, values):
        LOGGER.info("Executing query: '{}'".format(query % values))
        try:
            self._cursor.execute(query, values)
        except pg8000.core.OperationalError:
            self._connect()
            self.execute_query(query, values)
        self._connection.commit()
        return self._cursor.fetchall()
