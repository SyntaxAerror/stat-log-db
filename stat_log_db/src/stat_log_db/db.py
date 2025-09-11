import os
import uuid
import importlib
from typing import Any

# import sqlite3
from sqlalchemy import create_engine as sqla_create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

# from .exceptions import raise_auto_arg_type_error
from stat_log_db.data import get_data_from_file

from stat_log_db.modules.base import BaseModel


class Database():
    def __init__(self, options: dict[str, Any] = {}):
        # Validate arguments
        valid_options = {
            "db_name": str,
            "is_mem": bool,
            # "fkey_constraint": bool,
            "debug": bool
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
        # self._fkey_constraint: bool = options.get("fkey_constraint", True)
        self._debug: bool = options.get("debug", False)
        # SQLAlchemy engine
        self._engine: Engine | None = None

    # region Properties

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

    # @property
    # def fkey_constraint(self) -> bool:
    #     return self._fkey_constraint

    @property
    def debug(self) -> bool:
        return self._debug

    @property
    def engine(self) -> Engine:
        """
            Get the SQLAlchemy database engine.

            `self._engine` will be `None` if the database has not been initialized.
            In which case, calling `self.engine` (this property) will raise an error.
        """
        if self._engine is None:
            raise ValueError("Database engine is not initialized. Call 'init_db()' first.")
        if not isinstance(self._engine, Engine):
            raise TypeError(f"Database engine is not of type 'Engine', got '{type(self._engine).__name__}'.")
        return self._engine

    # endregion

    # region Initialization & Closure

    def init_db(self):
        """
            Initialize the database.
        """
        self._engine = sqla_create_engine(f"sqlite:///{self._db_file_name}")
        BaseModel.metadata.create_all(self.engine)

    def close_db(self):
        """
            Close the database.
        """
        self.engine.dispose()
        self._engine = None

    # def connect(self):
    #     """
    #         Create and return a new database connection.
    #     """
    #     connection = self.engine.connect()
    #     return connection

    # endregion

    # region Data Loading

    def load_data(self, module: str, file: str):
        """
            Load data from a file into the database.
        """
        project_root = os.path.dirname(os.path.abspath(__file__))
        module_file_path = f"{project_root}/modules/{module}/data/{file}"
        datas = get_data_from_file(module_file_path)
        with Session(self.engine) as session:
            for data in datas:
                metadata = data['metadata']
                external_id = metadata.get('external_id', None)
                model = metadata['model']
                module_path = f"stat_log_db.modules.{module}"
                model_module = importlib.import_module(module_path)
                model_class = getattr(model_module, model)
                vals = data['vals']
                vals['external_id'] = external_id
                instance = model_class(**vals)
                session.add(instance)
            session.commit()

    # endregion
