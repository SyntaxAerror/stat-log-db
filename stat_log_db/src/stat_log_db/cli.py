import os
import sys

from .parser import create_parser
from .db import Database


def main():
    """Main CLI entry point."""

    # TODO: Read info from pyproject.toml?
    parser = create_parser({
        "prog": "sldb",
        "description": "My CLI tool",
    }, "0.0.1")

    args = parser.parse_args()

    print(f"{args=}")

    sl_db = Database('sl_db.sqlite')

    sl_db.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, message TEXT)")
    sl_db.execute("INSERT INTO logs (message) VALUES (?)", ("Hello, world!",))
    sl_db.commit()
    sl_db.execute("SELECT * FROM logs")
    sql_logs = sl_db.fetchall()
    print(sql_logs)

if __name__ == "__main__":
    main()
