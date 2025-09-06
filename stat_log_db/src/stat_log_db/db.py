# from abc import ABC, abstractmethod
import re
import sqlite3
import uuid
from typing import Any


from .exceptions import raise_auto_arg_type_error


class Database():
    def __init__(self, options: dict[str, Any] = {}):
        # Validate arguments
        valid_options = {
            "db_name": str,
            "is_mem": bool,
            "fkey_constraint": bool
        }
        for opt, opt_type in options.items():
            if opt not in valid_options.keys():
                raise ValueError(f"Invalid option provided: '{opt}'. Must be one of {list(valid_options.keys())}.")
            expected_type = valid_options[opt]
            if not isinstance(opt_type, expected_type):
                raise TypeError(f"Option '{opt}' must be of type {expected_type.__name__}, got {type(opt_type).__name__}.")
        # Assign arguments to class attributes
        self._in_memory: bool = options.get("is_mem", False)
        self._is_file: bool = bool(not self._in_memory)
        self._db_name: str = options.get("db_name", str(uuid.uuid4()))
        self._db_file_name: str = ":memory:" if self._in_memory else self._db_name.replace(" ", "_")
        self._fkey_constraint: bool = options.get("fkey_constraint", True)
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
    def __init__(self, options: dict[str, Any] = {}):
        super().__init__(options=options)
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
    def __init__(self, options: dict[str, Any] = {}):
        super().__init__(options=options)
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
        self._connection = sqlite3.connect(self.db_file_name)
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
            raise ValueError("'query' argument of execute cannot be an empty string!")
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

    def _validate_sql_identifier(self, identifier: str, identifier_type: str = "identifier") -> str:
        """
        Validate and sanitize SQL identifiers (table names, column names) to prevent SQL injection.
        Args:
            identifier: The identifier to validate
            identifier_type: Type of identifier for error messages (e.g., "table name", "column name")
        Returns:
            The validated identifier
        Raises:
            ValueError: If the identifier is invalid or potentially dangerous
        """
        if not isinstance(identifier, str):
            raise TypeError(f"SQL {identifier_type} must be a string, got {type(identifier).__name__}")
        if len(identifier) == 0:
            raise ValueError(f"SQL {identifier_type} cannot be empty")
        # Check for valid identifier pattern: starts with letter/underscore, contains only alphanumeric/underscore
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(f"Invalid SQL {identifier_type}: '{identifier}'. Must start with letter or underscore and contain only letters, numbers, and underscores.")
        # Check against SQLite reserved words (common ones that could cause issues)
        reserved_words = {
            'abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
            'attach', 'autoincrement', 'before', 'begin', 'between', 'by', 'cascade', 'case',
            'cast', 'check', 'collate', 'column', 'commit', 'conflict', 'constraint', 'create',
            'cross', 'current', 'current_date', 'current_time', 'current_timestamp', 'database',
            'default', 'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct', 'do',
            'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive', 'exists', 'explain',
            'fail', 'filter', 'following', 'for', 'foreign', 'from', 'full', 'glob', 'group',
            'having', 'if', 'ignore', 'immediate', 'in', 'index', 'indexed', 'initially', 'inner',
            'insert', 'instead', 'intersect', 'into', 'is', 'isnull', 'join', 'key', 'left',
            'like', 'limit', 'match', 'natural', 'no', 'not', 'notnull', 'null', 'of', 'offset',
            'on', 'or', 'order', 'outer', 'over', 'partition', 'plan', 'pragma', 'preceding',
            'primary', 'query', 'raise', 'range', 'recursive', 'references', 'regexp', 'reindex',
            'release', 'rename', 'replace', 'restrict', 'right', 'rollback', 'row', 'rows',
            'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to', 'transaction',
            'trigger', 'unbounded', 'union', 'unique', 'update', 'using', 'vacuum', 'values',
            'view', 'virtual', 'when', 'where', 'window', 'with', 'without'
        }
        if identifier.lower() in reserved_words:
            raise ValueError(f"SQL {identifier_type} '{identifier}' is a reserved word and cannot be used")
        return identifier

    def _escape_sql_identifier(self, identifier: str) -> str:
        """
        Escape SQL identifier by wrapping in double quotes and escaping any internal quotes.
        This should only be used after validation.
        """
        # Escape any double quotes in the identifier by doubling them
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def create_table(self, table_name: str, columns: list[tuple[str, str]], temp_table: bool = True, raise_if_exists: bool = True):
        # Validate table_name argument
        if not isinstance(table_name, str):
            raise_auto_arg_type_error("table_name")
        if len(table_name) == 0:
            raise ValueError("'table_name' argument of create_table cannot be an empty string!")
        # Validate temp_table argument
        if not isinstance(temp_table, bool):
            raise_auto_arg_type_error("temp_table")
        if not isinstance(raise_if_exists, bool):
            raise_auto_arg_type_error("raise_if_exists")
        # Validate columns argument
        if (not isinstance(columns, list)) or (not all(
            isinstance(col, tuple) and len(col) == 2
            and isinstance(col[0], str)
            and isinstance(col[1], str)
        for col in columns)):
            raise_auto_arg_type_error("columns")
        # Validate and sanitize table name
        validated_table_name = self._validate_sql_identifier(table_name, "table name")
        escaped_table_name = self._escape_sql_identifier(validated_table_name)
        # Check if table already exists using parameterized query
        if raise_if_exists:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (validated_table_name,))
            if self.cursor.fetchone() is not None:
                raise ValueError(f"Table '{validated_table_name}' already exists.")
        # Validate and construct columns portion of query
        validated_columns = []
        for col_name, col_type in columns:
            # Validate column name
            validated_col_name = self._validate_sql_identifier(col_name, "column name")
            escaped_col_name = self._escape_sql_identifier(validated_col_name)
            # Validate column type - allow only safe, known SQLite types
            allowed_types = {
                'TEXT', 'INTEGER', 'REAL', 'BLOB', 'NUMERIC',
                'VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR',
                'CLOB', 'DATE', 'DATETIME', 'TIMESTAMP',
                'BOOLEAN', 'DECIMAL', 'DOUBLE', 'FLOAT',
                'INT', 'BIGINT', 'SMALLINT', 'TINYINT'
            }
            # Allow type specifications with length/precision (e.g., VARCHAR(50), DECIMAL(10,2))
            base_type = re.match(r'^([A-Z]+)', col_type.upper())
            if not base_type or base_type.group(1) not in allowed_types:
                raise ValueError(f"Unsupported column type: '{col_type}'. Must be one of: {', '.join(sorted(allowed_types))}")
            # Basic validation for type specification format
            if not re.match(r'^[A-Z]+(\([0-9,\s]+\))?$', col_type.upper()):
                raise ValueError(f"Invalid column type format: '{col_type}'")
            validated_columns.append(f"{escaped_col_name} {col_type.upper()}")
        columns_qstr = ",\n                ".join(validated_columns)
        # Assemble full query with escaped identifiers
        temp_keyword = " TEMPORARY" if temp_table else ""
        query = f"""CREATE{temp_keyword} TABLE IF NOT EXISTS {escaped_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns_qstr}
            );"""
        self.execute(query)

    def drop_table(self, table_name: str, raise_if_not_exists: bool = False):
        # Validate table_name argument
        if not isinstance(table_name, str):
            raise_auto_arg_type_error("table_name")
        if len(table_name) == 0:
            raise ValueError("'table_name' argument of drop_table cannot be an empty string!")
        if not isinstance(raise_if_not_exists, bool):
            raise_auto_arg_type_error("raise_if_not_exists")
        # Validate and sanitize table name
        validated_table_name = self._validate_sql_identifier(table_name, "table name")
        escaped_table_name = self._escape_sql_identifier(validated_table_name)
        # Check if table exists using parameterized query
        if raise_if_not_exists:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (validated_table_name,))
            if self.cursor.fetchone() is None:
                raise ValueError(f"Table '{validated_table_name}' does not exist.")
        # Execute DROP statement with escaped identifier
        self.cursor.execute(f"DROP TABLE IF EXISTS {escaped_table_name};")

    # def read(self):
    #     pass

    # def write(self):
    #     pass

    # def create(self):
    #     pass

    # def unlink(self):
    #     pass


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
