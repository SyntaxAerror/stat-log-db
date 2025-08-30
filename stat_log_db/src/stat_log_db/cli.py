import os
import sys

from .parser import create_parser


def main():
    """Main CLI entry point."""

    # TODO: Read info from pyproject.toml?
    parser = create_parser({
        "prog": "sldb",
        "description": "My CLI tool",
    }, "0.0.1")

    args = parser.parse_args()

    print(f"{args=}")


if __name__ == "__main__":
    main()
