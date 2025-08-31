# from abc import ABC, abstractmethod
import sqlite3
import uuid


from .exceptions import raise_auto_arg_type_error


class Database():
    def __init__(self, db_name: str | None = None, is_mem: bool = False, fkey_constraint: bool = True):
        # Validate arguments
        # database name
        if db_name is None:
            self._db_name = str(uuid.uuid4())
        elif not isinstance(db_name, str):
            raise_auto_arg_type_error("db_name")
        else:
            self._db_name = db_name
        # is memory or file database
        if not isinstance(is_mem, bool):
            raise_auto_arg_type_error("is_mem")
        self._in_memory = is_mem
        self._is_file = not is_mem
        # database file name
        if is_mem:
            self._db_file_name = ":memory:"
        else:
            self._db_file_name = self._db_name.replace(" ", "_")
        if not isinstance(fkey_constraint, bool):
            raise_auto_arg_type_error("fkey_constraint")
        self._fkey_constraint = fkey_constraint
        # Keep track of active connections (to ensure that they are closed)
        self._connections: dict[str, BaseConnection] = dict()

    @property
    def name(self) -> str:
        return self._db_name

    @property
    def file_name(self) -> str:
        return self._db_file_name

    @property
    def in_memory(self) -> bool:
        return self._in_memory

    @property
    def is_file(self) -> bool:
        return self._is_file

    @property
    def fkey_constraint(self) -> bool:
        return self._fkey_constraint

    def check_connection_integrity(self, connection: 'str | BaseConnection', skip_registry_type_check: bool = False):
        """
        Check the integrity of a given connection's registration.
        The connection to be checked can be passed as an UID string or a connection object (instance of BaseConnection).
        """
        if not isinstance(skip_registry_type_check, bool):
            raise_auto_arg_type_error("skip_registry_type_check")
        connection_is_uid_str = isinstance(connection, str)
        connection_is_obj = isinstance(connection, BaseConnection)
        if (not connection_is_uid_str) and (not connection_is_obj):
            raise_auto_arg_type_error("connection")
        if self._connections is None or len(self._connections) == 0:
            raise ValueError(f"Connection {connection.uid if connection_is_obj else connection} is not registered, as Connection Registry contains no connections.")
        # Check that the registry is of the expected type
        if (not skip_registry_type_check) and (not isinstance(self._connections, dict)):
            raise TypeError(f"Expected connection registry to be a dictionary but it was {type(self._connections).__name__}")
        # If the passed-in connection is a uid string,
        # search the registry keys for that uid string.
        # Check that a matching connection is found,
        # that it has a valid UID, and that it is registered
        # under the uid that it has (registry key = found connection's uid).
        if connection_is_uid_str:
            if len(connection) == 0:
                raise ValueError("Connection UID string is empty.")
            found_connection = self._connections.get(connection, None)
            if found_connection is None:
                raise ValueError(f"Connection '{connection}' is not registered.")
            if not isinstance(found_connection.uid, str):
                raise TypeError(f"Expected the found connection's uid to be str, got {type(found_connection.uid).__name__} instead.")
            if len(found_connection.uid) == 0:
                raise ValueError("Found connection's uid string is empty.")
            if found_connection.uid != connection:
                raise ValueError(f"Connection '{connection}' is registered under non-matching uid: {found_connection.uid}")
        # If the passed-in connection is a BaseConnection object,
        # check that it has a valid uid and that it's UID is in the registry
        elif connection_is_obj:
            if not isinstance(connection.uid, str):
                raise TypeError(f"Expected the connection's uid to be str, got {type(connection.uid).__name__} instead.")
            if connection.uid not in self._connections:
                raise ValueError(f"Connection '{connection.uid}' is not registered, or is registered under the wrong uid.")

    def check_connection_registry_integrity(self, skip_registry_type_check: bool = False):
        """
        Check the integrity of the connection registry.
        If not all connections are registered, no error is raised.
        """
        if not isinstance(skip_registry_type_check, bool):
            raise_auto_arg_type_error("skip_registry_type_check")
        # Check that the registry is of the expected type
        if (not skip_registry_type_check) and (not isinstance(self._connections, dict)):
            raise TypeError(f"Expected connection registry to be a dictionary but it was {type(self._connections).__name__}")
        # If there are no connections, nothing to check
        if len(self._connections) == 0:
            return
        # Check that all registered connections are registered under a UID of the correct type and are instances of BaseConnection
        if any((not isinstance(uid, str)) or (not isinstance(conn, BaseConnection)) for uid, conn in self._connections.items()):
            raise TypeError("All connections must be registered by their UID string and be instances of BaseConnection.")
        # Perform individual connection integrity checks
        for uid in self._connections.keys():
            self.check_connection_integrity(uid, skip_registry_type_check=True) # Registry type already checked

    def _register_connection(self):
        """
        Creates a new database connection object and registers it.
        Does not open the connection.
        """
        connection = BaseConnection(self)
        self._connections[connection.uid] = connection
        self.check_connection_integrity(connection)
        return connection

    def _unregister_connection(self, connection: 'str | BaseConnection'):
        """
        Unregister a database connection object.
        Does not close it.
        """
        connection_is_obj = isinstance(connection, BaseConnection)
        if (not isinstance(connection, str)) and (not connection_is_obj):
            raise_auto_arg_type_error("connection")
        connection_uid_str = connection.uid if connection_is_obj else connection
        self.check_connection_integrity(connection_uid_str)
        # TODO: consider implementing garbage collector ref-count check
        del self._connections[connection_uid_str]

    def init_db(self, commit_fkey: bool = True) -> 'BaseConnection':
        if not isinstance(commit_fkey, bool):
            raise_auto_arg_type_error("commit_fkey")
        connection = self._register_connection()
        connection.open()
        connection.enforce_foreign_key_constraints(commit_fkey)
        return connection

    def init_db_auto_close(self):
        if self.in_memory:
            raise ValueError("In-memory databases cease to exist upon closure.")
        # don't bother to commit fkey constraint because close() will commit before connection closure
        connection = self.init_db(False)
        connection.close()
        self._unregister_connection(connection.uid)

    def close_db(self):
        uids = []
        self.check_connection_registry_integrity()
        for uid, connection in self._connections.items():
            connection.close()
            uids.append(uid)
        for uid in uids:
            self._unregister_connection(uid)
        if not len(self._connections) == 0:
            raise RuntimeError("Not all connections were closed properly.")
        self._connections = dict()


class MemDB(Database):
    def __init__(self, db_name: str | None = None, is_mem: bool = False, fkey_constraint: bool = True):
        super().__init__(db_name, is_mem, fkey_constraint)
        if not self.in_memory:
            raise ValueError("MemDB can only be used for in-memory databases.")

    def check_connection_registry_integrity(self, skip_registry_type_check: bool = False):
        """
        Check the integrity of the connection registry.
        Implements early raise if more than one connection is found,
        since in-memory databases can only have one connection.
        """
        if not isinstance(skip_registry_type_check, bool):
            raise_auto_arg_type_error("skip_registry_type_check")
        if not skip_registry_type_check:
            if not isinstance(self._connections, dict):
                raise TypeError(f"Expected connection registry to be a dictionary but it was {type(self._connections).__name__}")
            if (num_connections := len(self._connections)) > 1:
                raise ValueError(f"In-memory databases can only have one active connection Found {num_connections}.")
        return super().check_connection_registry_integrity(skip_registry_type_check=True) # Registry type already checked

    def init_db_auto_close(self):
        raise ValueError("In-memory databases cease to exist upon closure.")


class FileDB(Database):
    def __init__(self, db_name: str | None = None, fkey_constraint: bool = True):
        super().__init__(db_name, fkey_constraint)
        if not self.is_file:
            raise ValueError("FileDB can only be used for file-based databases.")


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
    def db_file_name(self):
        return self._db._db_file_name

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
        self._db.check_connection_integrity(self) # raises error if not registered
        return True

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
        Performs no checks/validation. Prefer `execute` unless you need raw access.
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

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def create_table(self, table_name: str, columns: list[tuple[str, str]], temp_table: bool = True, raise_if_exists: bool = True):
        # Validate table_name argument
        if not isinstance(table_name, str):
            raise_auto_arg_type_error("table_name")
        if len(table_name) == 0:
            raise ValueError(f"'table_name' argument of create_table cannot be an empty string!")
        if not isinstance(raise_if_exists, bool):
            raise_auto_arg_type_error("raise_if_exists")
        # Check if table already exists
        if raise_if_exists:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            if self.cursor.fetchone() is not None:
                raise ValueError(f"Table '{table_name}' already exists.")
        # Validate temp_table argument
        if not isinstance(temp_table, bool):
            raise_auto_arg_type_error("temp_table")
        # Validate columns argument
        if (not isinstance(columns, list)) or (not all(
            isinstance(col, tuple) and len(col) == 2
            and isinstance(col[0], str)
            and isinstance(col[1], str)
        for col in columns)):
            raise_auto_arg_type_error("columns")
        # Construct columns portion of query
        # TODO: construct parameters for columns rather than f-string to prevent SQL injection
        columns_qstr = ""
        for col in columns:
            columns_qstr += f"{col[0]} {col[1]},\n"
        columns_qstr = columns_qstr.rstrip(",\n") # Remove trailing comma and newline
        # Assemble full query
        query = f"""--sql
            CREATE{" TEMPORARY" if temp_table else ""} TABLE IF NOT EXISTS '{table_name}' (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns_qstr}
            );
        """
        self.execute(query)

    def drop_table(self, table_name: str, raise_if_not_exists: bool = False):
        # Validate table_name argument
        if not isinstance(table_name, str):
            raise_auto_arg_type_error("table_name")
        if len(table_name) == 0:
            raise ValueError(f"'table_name' argument of drop_table cannot be an empty string!")
        if not isinstance(raise_if_not_exists, bool):
            raise_auto_arg_type_error("raise_if_not_exists")
        if raise_if_not_exists:
            self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if self.cursor.fetchone() is None:
                raise ValueError(f"Table '{table_name}' does not exist.")
        self.cursor.execute(f"DROP TABLE IF EXISTS '{table_name}';")

    # def read(self):
        

    # def write(self):
        

    # def create(self):
        

    # def unlink(self):
        


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
