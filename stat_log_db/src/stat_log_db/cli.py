import os
import sys

from .parser import create_parser
from .db import Database, MemDB, FileDB, BaseConnection


def main():
    """Main CLI entry point."""

    # TODO: Read info from pyproject.toml?
    parser = create_parser({
        "prog": "sldb",
        "description": "My CLI tool",
    }, "0.0.1")

    args = parser.parse_args()

    # print(f"{args=}")

    sl_db = MemDB(":memory:", True, True)
    con = sl_db.init_db(True)
    con.create_table("test", [('notes', 'TEXT')], False, True)
    con.execute("INSERT INTO test (notes) VALUES (?);", ("Hello world!",))
    con.commit()
    con.execute("SELECT * FROM test;")
    sql_logs = con.fetchall()
    print(sql_logs)
    con.drop_table("test", True)
    sl_db.close_db()
    if sl_db.is_file:
        os.remove(sl_db.file_name)

if __name__ == "__main__":
    main()
