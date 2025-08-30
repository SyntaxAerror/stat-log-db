import sqlite3

from .exceptions import raise_type_error_with_signature


class Database:
    def __init__(self, db_name: str):
        self._db_name = db_name
        self._connection = sqlite3.connect(self._db_name)
        self._cursor = self._connection.cursor()

    def __del__(self): # TODO: Is this the right way to handle database closure?
        self._connection.close()

    @property
    def db_name(self):
        return self._db_name

    @property
    def connection(self):
        return self._connection

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self._connection.commit()

    def execute(self, query: str, params: tuple = ()):
        if not isinstance(query, str):
            raise_type_error_with_signature("query")
        if not isinstance(params, tuple):
            raise_type_error_with_signature("params")
        self._cursor.execute(query, params)
        self.commit()

    def fetchall(self):
        return self._cursor.fetchall()
