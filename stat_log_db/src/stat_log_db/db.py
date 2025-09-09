# from abc import ABC, abstractmethod
import re
import sqlite3
import uuid
from typing import Any

from sqlalchemy import create_engine, text, Engine, Connection as SQLAConnection, MetaData, Table, Column
from sqlalchemy import Integer, String, Text, Boolean, Float, DateTime, LargeBinary
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
        Validate SQL identifiers with reserved word checking and basic format validation.
        SQLAlchemy handles parameterization, but we still validate identifier safety.
        """
        if not isinstance(identifier, str):
            raise TypeError(f"SQL {identifier_type} must be a string, got {type(identifier).__name__}")
        if len(identifier) == 0:
            raise ValueError(f"SQL {identifier_type} cannot be empty")
        
        # Basic format validation
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(f"Invalid SQL {identifier_type}: '{identifier}'. Must start with letter or underscore and contain only letters, numbers, and underscores.")
        
        # Check against SQL reserved words
        sql_reserved_words = {
            'ABORT', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALTER', 'ANALYZE', 'AND', 'AS', 'ASC',
            'ATTACH', 'AUTOINCREMENT', 'BEFORE', 'BEGIN', 'BETWEEN', 'BY', 'CASCADE', 'CASE',
            'CAST', 'CHECK', 'COLLATE', 'COLUMN', 'COMMIT', 'CONFLICT', 'CONSTRAINT', 'CREATE',
            'CROSS', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'DATABASE', 'DEFAULT',
            'DEFERRABLE', 'DEFERRED', 'DELETE', 'DESC', 'DETACH', 'DISTINCT', 'DROP', 'EACH',
            'ELSE', 'END', 'ESCAPE', 'EXCEPT', 'EXCLUSIVE', 'EXISTS', 'EXPLAIN', 'FAIL', 'FOR',
            'FOREIGN', 'FROM', 'FULL', 'GLOB', 'GROUP', 'HAVING', 'IF', 'IGNORE', 'IMMEDIATE',
            'IN', 'INDEX', 'INDEXED', 'INITIALLY', 'INNER', 'INSERT', 'INSTEAD', 'INTERSECT',
            'INTO', 'IS', 'ISNULL', 'JOIN', 'KEY', 'LEFT', 'LIKE', 'LIMIT', 'MATCH', 'NATURAL',
            'NO', 'NOT', 'NOTNULL', 'NULL', 'OF', 'OFFSET', 'ON', 'OR', 'ORDER', 'OUTER', 'PLAN',
            'PRAGMA', 'PRIMARY', 'QUERY', 'RAISE', 'RECURSIVE', 'REFERENCES', 'REGEXP', 'REINDEX',
            'RELEASE', 'RENAME', 'REPLACE', 'RESTRICT', 'RIGHT', 'ROLLBACK', 'ROW', 'SAVEPOINT',
            'SELECT', 'SET', 'TABLE', 'TEMP', 'TEMPORARY', 'THEN', 'TO', 'TRANSACTION', 'TRIGGER',
            'UNION', 'UNIQUE', 'UPDATE', 'USING', 'VACUUM', 'VALUES', 'VIEW', 'VIRTUAL', 'WHEN',
            'WHERE', 'WITH', 'WITHOUT'
        }
        
        if identifier.upper() in sql_reserved_words:
            raise ValueError(f"SQL {identifier_type} '{identifier}' is a reserved word")
        
        return identifier

    def _validate_column_type(self, col_type: str) -> str:
        """
        Validate SQLite column type specification.
        Returns the normalized (uppercase) column type.
        """
        if not isinstance(col_type, str):
            raise TypeError(f"Column type must be a string, got {type(col_type).__name__}")
        
        # Normalize to uppercase
        normalized_type = col_type.upper().strip()
        
        # Define allowed SQLite types
        allowed_types = {
            'TEXT', 'INTEGER', 'REAL', 'BLOB', 'NUMERIC',
            'VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR', 
            'CLOB', 'DATE', 'DATETIME', 'TIMESTAMP',
            'BOOLEAN', 'DECIMAL', 'DOUBLE', 'FLOAT',
            'INT', 'BIGINT', 'SMALLINT', 'TINYINT'
        }
        
        # Extract base type (before any parentheses)
        base_type_match = re.match(r'^([A-Z]+)', normalized_type)
        if not base_type_match:
            raise ValueError(f"Invalid column type format: '{col_type}'")
        
        base_type = base_type_match.group(1)
        if base_type not in allowed_types:
            raise ValueError(f"Unsupported column type: '{col_type}'. Must be one of: {', '.join(sorted(allowed_types))}")
        
        # Validate full type specification format (allowing precision/length specifiers)
        if not re.match(r'^[A-Z]+(\([0-9,\s]+\))?$', normalized_type):
            raise ValueError(f"Invalid column type format: '{col_type}'. Use format like 'VARCHAR(50)' or 'DECIMAL(10,2)'")
        
        return normalized_type

    def _build_column_definition(self, col_name: str, col_type: str) -> str:
        """
        Build a column definition string with proper validation and escaping.
        Returns formatted column definition for CREATE TABLE statement.
        """
        validated_col_name = self._validate_identifier(col_name, "column name")
        validated_col_type = self._validate_column_type(col_type)
        
        # Use SQLite standard double-quote escaping for identifiers
        escaped_col_name = f'"{validated_col_name}"'
        
        return f"{escaped_col_name} {validated_col_type}"

    def create_table(self, table_name: str, columns: list[tuple[str, str]], temp_table: bool = True, raise_if_exists: bool = True):
        """
        Create a new table using SQLAlchemy with proper validation and escaping.
        
        Args:
            table_name: Name of the table to create
            columns: List of (column_name, column_type) tuples
            temp_table: Whether to create a temporary table
            raise_if_exists: Whether to raise an error if table already exists
        """
        # Validate arguments
        if not isinstance(table_name, str):
            raise_auto_arg_type_error("table_name")
        if len(table_name) == 0:
            raise ValueError("'table_name' argument cannot be an empty string")
        if not isinstance(temp_table, bool):
            raise_auto_arg_type_error("temp_table")
        if not isinstance(raise_if_exists, bool):
            raise_auto_arg_type_error("raise_if_exists")
        if not isinstance(columns, list) or not all(
            isinstance(col, tuple) and len(col) == 2
            and isinstance(col[0], str) and isinstance(col[1], str)
            for col in columns
        ):
            raise_auto_arg_type_error("columns")
        
        # Validate and normalize table name
        validated_table_name = self._validate_identifier(table_name, "table name")
        escaped_table_name = f'"{validated_table_name}"'
        
        # Check if table already exists using SQLAlchemy parameterized query
        if raise_if_exists:
            check_query = text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name")
            result = self.connection.execute(check_query, {"table_name": validated_table_name})
            if result.fetchone() is not None:
                raise ValueError(f"Table '{validated_table_name}' already exists")
        
        # Build column definitions using helper method
        column_definitions = []
        for col_name, col_type in columns:
            column_def = self._build_column_definition(col_name, col_type)
            column_definitions.append(column_def)
        
        # Format column definitions for query
        columns_clause = ",\n                ".join(column_definitions)
        
        # Build CREATE TABLE statement
        temp_keyword = " TEMPORARY" if temp_table else ""
        create_query = f"""CREATE{temp_keyword} TABLE IF NOT EXISTS {escaped_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns_clause}
            )"""
        
        # Execute using SQLAlchemy's text() construct for safe execution
        self.connection.execute(text(create_query))

    def _map_sqlite_type_to_sqlalchemy(self, sqlite_type: str):
        """
        Map SQLite type strings to SQLAlchemy column types.
        Returns appropriate SQLAlchemy column type class.
        """
        # Normalize the type string
        normalized_type = sqlite_type.upper().strip()
        base_type_match = re.match(r'^([A-Z]+)', normalized_type)
        if not base_type_match:
            raise ValueError(f"Invalid SQLite type format: '{sqlite_type}'")
        base_type = base_type_match.group(1)
        
        # SQLite to SQLAlchemy type mapping
        type_mapping = {
            'TEXT': Text,
            'VARCHAR': String,
            'CHAR': String,
            'NVARCHAR': String,
            'NCHAR': String,
            'CLOB': Text,
            'INTEGER': Integer,
            'INT': Integer,
            'BIGINT': Integer,
            'SMALLINT': Integer,
            'TINYINT': Integer,
            'REAL': Float,
            'DOUBLE': Float,
            'FLOAT': Float,
            'NUMERIC': Float,
            'DECIMAL': Float,
            'BOOLEAN': Boolean,
            'DATE': DateTime,
            'DATETIME': DateTime,
            'TIMESTAMP': DateTime,
            'BLOB': LargeBinary,
        }
        
        if base_type not in type_mapping:
            raise ValueError(f"Cannot map SQLite type '{sqlite_type}' to SQLAlchemy type")
        
        sqlalchemy_type = type_mapping[base_type]
        
        # Handle length specifications for string types
        if base_type in ('VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR') and '(' in normalized_type:
            length_match = re.search(r'\((\d+)\)', normalized_type)
            if length_match:
                length = int(length_match.group(1))
                return sqlalchemy_type(length)
        
        return sqlalchemy_type()

    def create_table_with_sqlalchemy_ddl(self, table_name: str, columns: list[tuple[str, str]], temp_table: bool = True, raise_if_exists: bool = True):
        """
        Create a table using SQLAlchemy's Table and MetaData objects for enhanced DDL capabilities.
        This provides better type safety and integration with SQLAlchemy's ORM features.
        
        Args:
            table_name: Name of the table to create
            columns: List of (column_name, column_type) tuples
            temp_table: Whether to create a temporary table
            raise_if_exists: Whether to raise an error if table already exists
        """
        # Validate arguments (reuse validation from create_table)
        if not isinstance(table_name, str) or len(table_name) == 0:
            raise ValueError("table_name must be a non-empty string")
        if not isinstance(temp_table, bool):
            raise_auto_arg_type_error("temp_table")
        if not isinstance(raise_if_exists, bool):
            raise_auto_arg_type_error("raise_if_exists")
        if not isinstance(columns, list) or not all(
            isinstance(col, tuple) and len(col) == 2
            and isinstance(col[0], str) and isinstance(col[1], str)
            for col in columns
        ):
            raise_auto_arg_type_error("columns")
        
        # Validate table name
        validated_table_name = self._validate_identifier(table_name, "table name")
        
        # Check if table exists
        if raise_if_exists:
            check_query = text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name")
            result = self.connection.execute(check_query, {"table_name": validated_table_name})
            if result.fetchone() is not None:
                raise ValueError(f"Table '{validated_table_name}' already exists")
        
        # Create MetaData object
        metadata = MetaData()
        
        # Build SQLAlchemy Column objects
        sqlalchemy_columns = [
            Column('id', Integer, primary_key=True, autoincrement=True)
        ]
        
        for col_name, col_type in columns:
            validated_col_name = self._validate_identifier(col_name, "column name")
            validated_col_type = self._validate_column_type(col_type)
            
            # Map to SQLAlchemy type
            sqlalchemy_type = self._map_sqlite_type_to_sqlalchemy(validated_col_type)
            sqlalchemy_columns.append(Column(validated_col_name, sqlalchemy_type))
        
        # Create Table object
        if temp_table:
            # For temporary tables, fall back to raw SQL since SQLAlchemy doesn't have direct support
            temp_keyword = " TEMPORARY"
            column_defs = []
            for col in sqlalchemy_columns:
                if col.name == 'id':
                    column_defs.append(f'"{col.name}" {col.type.compile(self._db.engine.dialect)} PRIMARY KEY AUTOINCREMENT')
                else:
                    column_defs.append(f'"{col.name}" {col.type.compile(self._db.engine.dialect)}')
            
            columns_clause = ",\n                ".join(column_defs)
            escaped_table_name = f'"{validated_table_name}"'
            create_query = f"""CREATE{temp_keyword} TABLE IF NOT EXISTS {escaped_table_name} (
                {columns_clause}
            )"""
            self.connection.execute(text(create_query))
        else:
            # For regular tables, use SQLAlchemy's DDL capabilities
            table = Table(
                validated_table_name,
                metadata,
                *sqlalchemy_columns
            )
            
            # Create the table using SQLAlchemy DDL
            metadata.create_all(self._db.engine, tables=[table], checkfirst=not raise_if_exists)

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
