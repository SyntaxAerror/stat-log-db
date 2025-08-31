from abc import ABC, abstractmethod
import sqlite3
import uuid


from .exceptions import raise_auto_arg_type_error


class Database(ABC):
    def __init__(self, db_name: str | None = None, fkey_constraint: bool = True):
        # Validate arguments
        if not isinstance(db_name, (str, type(None))):
            raise_auto_arg_type_error("db_name")
        if not isinstance(fkey_constraint, bool):
            raise_auto_arg_type_error("fkey_constraint")
        self._db_name: str = ":memory:" if db_name is None else db_name
        self._in_memory: bool = bool(self._db_name == ":memory:")
        self._is_file: bool = bool(self._db_name != ":memory:")
        self._fkey_constraint: bool = fkey_constraint
        # Keep track of active connections (to ensure that they are closed)
        self._connections: dict[str, BaseConnection] = dict()

    @property
    def db_name(self):
        return self._db_name

    @property
    def db_in_memory(self):
        return self._in_memory

    @property
    def db_is_file(self):
        return self._is_file

    @property
    def db_fkey_constraint(self):
        return self._fkey_constraint

    def _register_connection(self):
        """
        Creates a new database connection object and registers it.
        Does not open the connection.
        """
        con = BaseConnection(self)
        self._connections[con.uid] = con
        return con

    def _unregister_connection(self, connection: 'str | BaseConnection'):
        """
        Unregister a database connection object.
        Does not close it.
        """
        connection_registry_key = None
        if isinstance(connection, str):
            connection_registry_key = connection
        elif isinstance(connection, BaseConnection):
            connection_registry_key = connection.uid
        else:
            raise_auto_arg_type_error("con")
        if (connection_registry_key is None) or (connection_registry_key not in self._connections):
            raise ValueError(f"Connection {connection} is not registered.")
        del self._connections[connection_registry_key]

    def init_db(self, close_connection: bool = True):
        if not isinstance(close_connection, bool):
            raise_auto_arg_type_error("close_connection")
        if self._in_memory and close_connection:
            raise ValueError("In-memory databases cease to exist upon closure.")
        connection = self._register_connection()
        connection.open()
        connection.enforce_foreign_key_constraints(False)
        if close_connection:
            connection.close()
            self._unregister_connection(connection)
        else:
            return connection

    def close_db(self):
        uids = []
        for uid, connection in self._connections.items():
            if not isinstance(connection, BaseConnection):
                raise TypeError(f"Expected connection to be BaseConnection, got {type(connection).__name__} instead.")
            if connection.uid != uid:
                raise ValueError(f"Connection {connection.uid} is registered under non-matching uid: {uid}")
            connection.close()
            uids.append(uid)
        for uid in uids:
            self._unregister_connection(uid)
        if not len(self._connections) == 0:
            raise RuntimeError("Not all connections were closed properly.")
        self._connections = dict()

    # @abstractmethod
    # def create_table(self):
    #     pass

    # @abstractmethod
    # def drop_table(self):
    #     pass

    # @abstractmethod
    # def read(self):
    #     pass

    # @abstractmethod
    # def write(self):
    #     pass

    # @abstractmethod
    # def create(self):
    #     pass

    # @abstractmethod
    # def unlink(self):
    #     pass


class BaseConnection:
    def __init__(self, db: Database):
        if not isinstance(db, Database):
            raise_auto_arg_type_error("db")
        self._db: Database = db
        self._id = str(uuid.uuid4())
        self._connection: sqlite3.Connection | None = None
        self._cursor: sqlite3.Cursor | None = None

    @property
    def db_name(self):
        return self._db._db_name

    @property
    def db_in_memory(self):
        return self._db._in_memory

    @property
    def db_is_file(self):
        return self._db._is_file

    @property
    def db_fkey_constraint(self):
        return self._db._fkey_constraint

    @property
    def uid(self):
        # TODO: Hash together the uuid, db_name, and possibly also the location in memory to ensure uniqueness?
        return self._id

    @property
    def registered(self):
        return self.uid in self._db._connections

    @property
    def connection(self):
        if self._connection is None:
            raise RuntimeError("Connection is not open.")
        if not isinstance(self._connection, sqlite3.Connection):
            raise TypeError(f"Expected self._connection to be sqlite3.Connection, got {type(self._connection).__name__} instead.")
        return self._connection

    @property
    def cursor(self):
        if self._cursor is None:
            raise RuntimeError("Cursor is not open.")
        if not isinstance(self._cursor, sqlite3.Cursor):
            raise TypeError(f"Expected self._cursor to be sqlite3.Cursor, got {type(self._cursor).__name__} instead.")
        return self._cursor

    def enforce_foreign_key_constraints(self, commit: bool = True):
        if not isinstance(commit, bool):
            raise_auto_arg_type_error("commit")
        if self.db_fkey_constraint:
            self.cursor.execute("PRAGMA foreign_keys = ON;")
            if commit:
                self.connection.commit()

    def _open(self):
        self._connection = sqlite3.connect(self.db_name)
        self._cursor = self._connection.cursor()

    def open(self):
        if isinstance(self._connection, sqlite3.Connection):
            raise RuntimeError("Connection is already open.")
        if not (self._connection is None):
            raise TypeError(f"Expected self._connection to be None, got {type(self._connection).__name__} instead.")
        self._open()

    def _close(self):
        self.cursor.close()
        self._cursor = None
        self.connection.close()
        self._connection = None

    def close(self):
        self.connection.commit()
        self._close()

    def _execute(self, query: str, parameters: tuple = ()):
        """
        Execute a SQL query with the given parameters.
        """
        result = self.cursor.execute(query, parameters)
        return result

    def execute(self, query: str, parameters: tuple | None = None):
        """
        Execute a SQL query with the given parameters.
        """
        # Validate query and parameters
        if not isinstance(query, str):
            raise_auto_arg_type_error("query")
        if len(query) == 0:
            raise ValueError(f"'query' argument of execute cannot be an empty string!")
        # Create a new space in memory that points to the same object that `parameters` points to
        params = parameters
        # If `params` points to None, update it to point to an empty tuple
        if params is None:
            params = tuple()
        # If `params` points to an object that isn't a tuple or None (per previous condition), raise a TypeError
        elif not isinstance(params, tuple):
            raise_auto_arg_type_error("parameters")
        # Execute query with `params`
        result = self._execute(query, params)
        return result

    def commit(self):
        self.connection.commit()

    def fetchall(self):
        return self.cursor.fetchall()


class Connection(BaseConnection):
    def __init__(self, db: Database):
        super().__init__(db)
        self.open()
        self.enforce_foreign_key_constraints(True)


class FileConnectionCtx(BaseConnection):
    def __init__(self, db: Database):
        super().__init__(db)
        if not self.db_is_file:
            raise ValueError("FileConnectionCtx can only be used with file-based databases.")

    def __enter__(self):
        self.open()
        self.enforce_foreign_key_constraints(True)
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()
