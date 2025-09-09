# from abc import ABC, abstractmethod
import re
import sqlite3
import uuid
from typing import Any

from sqlalchemy import create_engine, text, Engine, Connection as SQLAConnection
from sqlalchemy.engine import make_url
from sqlalchemy.pool import StaticPool, QueuePool

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
        # Create SQLAlchemy Engine with appropriate connection pooling
        self._engine = self._create_engine()

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
    
    @property
    def engine(self) -> Engine:
        """Get the SQLAlchemy Engine for this database."""
        return self._engine

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy Engine with appropriate configuration for SQLite."""
        if self._in_memory:
            # For in-memory databases, use StaticPool to ensure single connection
            # and prevent the database from being destroyed when connections close
            url = "sqlite:///:memory:"
            engine = create_engine(
                url,
                poolclass=StaticPool,
                pool_pre_ping=True,
                connect_args={
                    "check_same_thread": False,  # Allow sharing between threads
                    "isolation_level": None,     # Use autocommit mode
                },
                echo=False  # Set to True for SQL debugging
            )
        else:
            # For file databases, use QueuePool for better concurrency
            url = f"sqlite:///{self._db_file_name}"
            engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                connect_args={
                    "check_same_thread": False,
                    "isolation_level": None,
                },
                echo=False
            )
        return engine

    def create_connection(self) -> 'BaseConnection':
        """
        Creates a new database connection object using SQLAlchemy Engine.
        SQLAlchemy handles connection pooling and lifecycle management.
        """
        return BaseConnection(self)

    def init_db(self, commit_fkey: bool = True) -> 'BaseConnection':
        if not isinstance(commit_fkey, bool):
            raise_auto_arg_type_error("commit_fkey")
        connection = self.create_connection()
        connection.open()
        connection.enforce_foreign_key_constraints(commit_fkey)
        return connection

    def init_db_auto_close(self):
        if self.in_memory:
            raise ValueError("In-memory databases cease to exist upon closure.")
        # don't bother to commit fkey constraint because close() will commit before connection closure
        connection = self.init_db(False)
        connection.close()
        # SQLAlchemy automatically handles connection cleanup

    def close_db(self):
        """Close the database and dispose of the SQLAlchemy engine to clean up connection pool."""
        if hasattr(self, '_engine') and self._engine is not None:
            self._engine.dispose()


# class MemDB(Database):
#     def __init__(self, options: dict[str, Any] = {}):
#         super().__init__(options=options)
#         if not self.in_memory:
#             raise ValueError("MemDB can only be used for in-memory databases.")

#     def init_db_auto_close(self):
#         raise ValueError("In-memory databases cease to exist upon closure.")


# class FileDB(Database):
#     def __init__(self, options: dict[str, Any] = {}):
#         super().__init__(options=options)
#         if not self.is_file:
#             raise ValueError("FileDB can only be used for file-based databases.")


class BaseConnection:
    def __init__(self, db: Database):
        if not isinstance(db, Database):
            raise_auto_arg_type_error("db")
        self._db: Database = db
        self._connection: SQLAConnection | None = None

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
    def connection(self):
        if self._connection is None:
            raise RuntimeError("Connection is not open.")
        if not isinstance(self._connection, SQLAConnection):
            raise TypeError(f"Expected self._connection to be SQLAlchemy Connection, got {type(self._connection).__name__} instead.")
        return self._connection

    def enforce_foreign_key_constraints(self, commit: bool = True):
        if not isinstance(commit, bool):
            raise_auto_arg_type_error("commit")
        if self.db_fkey_constraint:
            self.connection.execute(text("PRAGMA foreign_keys = ON;"))
            if commit:
                self.connection.commit()

    def _open(self):
        """Open a new SQLAlchemy connection from the engine."""
        self._connection = self._db.engine.connect()

    def open(self):
        if self._connection is not None:
            raise RuntimeError("Connection is already open.")
        self._open()

    def _close(self):
        """Close the SQLAlchemy connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def close(self):
        if self._connection is not None:
            self.connection.commit()
        self._close()

    def _execute(self, query: str, parameters: tuple = ()):
        """
        Execute a SQL query with the given parameters using SQLAlchemy.
        Performs no checks/validation. Prefer `execute` unless you need raw access.
        """
        # Convert parameters tuple to dict for SQLAlchemy if needed
        if parameters:
            # Create numbered parameter dict for SQLAlchemy
            param_dict = {f"param_{i}": param for i, param in enumerate(parameters)}
            # Replace ? placeholders with :param_N format
            param_query = query
            for i in range(len(parameters)):
                param_query = param_query.replace("?", f":param_{i}", 1)
            result = self.connection.execute(text(param_query), param_dict)
        else:
            result = self.connection.execute(text(query))
        return result

    def execute(self, query: str, parameters: tuple | None = None):
        """
        Execute a SQL query with the given parameters using SQLAlchemy text() construct.
        Returns the SQLAlchemy Result object for chaining fetchone/fetchall operations.
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
        # Execute query with `params` and store the result for potential fetching
        self._last_result = self._execute(query, params)
        return self._last_result

    def commit(self):
        self.connection.commit()

    def fetchone(self):
        """Fetch one row from the last executed query result."""
        if not hasattr(self, '_last_result') or self._last_result is None:
            raise RuntimeError("No query has been executed. Call execute() first.")
        return self._last_result.fetchone()

    def fetchall(self):
        """Fetch all rows from the last executed query result."""
        if not hasattr(self, '_last_result') or self._last_result is None:
            raise RuntimeError("No query has been executed. Call execute() first.")
        return self._last_result.fetchall()

    def _validate_identifier(self, identifier: str, identifier_type: str = "identifier") -> str:
        """
        Basic validation for identifiers. SQLAlchemy handles SQL injection protection.
        """
        if not isinstance(identifier, str):
            raise TypeError(f"SQL {identifier_type} must be a string, got {type(identifier).__name__}")
        if len(identifier) == 0:
            raise ValueError(f"SQL {identifier_type} cannot be empty")
        # Basic validation - SQLAlchemy will handle the rest
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(f"Invalid SQL {identifier_type}: '{identifier}'. Must start with letter or underscore and contain only letters, numbers, and underscores.")
        return identifier

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
        
        # Validate table name using basic validation
        validated_table_name = self._validate_identifier(table_name, "table name")
        
        # Check if table already exists using SQLAlchemy parameterized query
        if raise_if_exists:
            result = self.connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"), 
                {"table_name": validated_table_name}
            )
            if result.fetchone() is not None:
                raise ValueError(f"Table '{validated_table_name}' already exists.")
        
        # Validate columns and build column definitions
        column_definitions = []
        for col_name, col_type in columns:
            # Validate column name
            validated_col_name = self._validate_identifier(col_name, "column name")
            
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
            
            # Use double quotes for identifier escaping (SQLite standard)
            escaped_col_name = f'"{validated_col_name}"'
            column_definitions.append(f"{escaped_col_name} {col_type.upper()}")
        
        columns_qstr = ",\n                ".join(column_definitions)
        
        # Build CREATE TABLE statement with proper identifier escaping
        temp_keyword = " TEMPORARY" if temp_table else ""
        escaped_table_name = f'"{validated_table_name}"'
        
        query = f"""CREATE{temp_keyword} TABLE IF NOT EXISTS {escaped_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns_qstr}
            )"""
        
        # Execute using SQLAlchemy's text() construct
        self.connection.execute(text(query))

    def drop_table(self, table_name: str, raise_if_not_exists: bool = False):
        # Validate table_name argument
        if not isinstance(table_name, str):
            raise_auto_arg_type_error("table_name")
        if len(table_name) == 0:
            raise ValueError("'table_name' argument of drop_table cannot be an empty string!")
        if not isinstance(raise_if_not_exists, bool):
            raise_auto_arg_type_error("raise_if_not_exists")
        
        # Validate table name using basic validation
        validated_table_name = self._validate_identifier(table_name, "table name")
        
        # Check if table exists using SQLAlchemy parameterized query
        if raise_if_not_exists:
            result = self.connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"), 
                {"table_name": validated_table_name}
            )
            if result.fetchone() is None:
                raise ValueError(f"Table '{validated_table_name}' does not exist.")
        
        # Execute DROP TABLE query with proper identifier escaping
        escaped_table_name = f'"{validated_table_name}"'
        query = f"DROP TABLE IF EXISTS {escaped_table_name}"
        self.connection.execute(text(query))

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
