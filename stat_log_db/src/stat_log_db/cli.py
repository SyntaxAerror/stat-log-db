import os
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
    with Session(sl_db.engine) as session:
        info_type = LogType(
            name="INFO"
        )
        session.add(info_type)
        session.commit()
        info_level = LogLevel(
            name="INFO"
        )
        session.add(info_level)
        session.commit()
        hello_world = Log(
            type_id=1,
            level_id=1,
            message="Hello, World!"
        )
        session.add(hello_world)
        session.commit()
        logs = select(Log).where(Log.id == 1)
        for log in session.scalars(logs):
            print(f"{log.id=}, {log.type_id=}, {log.level_id=}, {log.message=}")


if __name__ == "__main__":
    main()
