import os
import sys

from .parser import create_parser
from .db import Database, BaseConnection


def main():
    """Main CLI entry point."""

    # TODO: Read info from pyproject.toml?
    parser = create_parser({
        "prog": "sldb",
        "description": "My CLI tool",
    }, "0.0.1")

    args = parser.parse_args()

    # print(f"{args=}")

    db_filename = 'sl_db.sqlite'
    sl_db = Database(db_filename)
    con = sl_db.init_db(False)
    if isinstance(con, BaseConnection):
        con.execute("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, message TEXT);")
        con.execute("INSERT INTO logs (message) VALUES (?);", ("Hello, world!",))
        con.commit()
        con.execute("SELECT * FROM logs;")
        sql_logs = con.fetchall()
        print(sql_logs)

    sl_db.close_db()
    # os.remove(db_filename)

if __name__ == "__main__":
    main()
