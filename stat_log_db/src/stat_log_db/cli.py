# import os
# import sys

from sqlalchemy import select
from sqlalchemy.orm import Session

# from .parser import create_parser
from stat_log_db.db import Database as DB
from stat_log_db.modules.log import Log, LogType, LogLevel


def main():
    """Main CLI entry point."""

    # TODO: Read info from pyproject.toml?
    # parser = create_parser({
    #     "prog": "sldb",
    #     "description": "My CLI tool",
    # }, "0.0.1")

    # args = parser.parse_args()

    # print(f"{args=}")

    sl_db = DB({
        "is_mem": True
    })
    sl_db.init_db()
    sl_db.load_data("log", "log_levels.xml")
    with Session(sl_db.engine) as session:
        info_level_query = select(LogLevel).where(LogLevel.external_id == 'log_level_info')
        info_level = session.scalar(info_level_query)
        if info_level is None:
            raise ValueError("LogLevel with external_id 'log_level_info' not found.")
        hello_world = Log(
            level_id=info_level.id,
            name="Hello World",
            message="Hello, World!"
        )
        session.add(hello_world)
        session.commit()
        logs = select(Log)
        for log in session.scalars(logs):
            print(f"{log.external_id=},\n{log.id=}, {log.name=},\n{log.type_id=}, {log.level_id=},\n{log.message=}")


if __name__ == "__main__":
    main()
