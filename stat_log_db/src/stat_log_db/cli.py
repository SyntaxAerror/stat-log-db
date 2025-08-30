import os
import sys
import argparse
from dotenv import load_dotenv

from .exceptions import raise_type_error_with_signature

load_dotenv()


def create_parser(parser_args: dict, version: str | int="0.0.1") -> argparse.ArgumentParser:
    """Create the main argument parser."""
    # Validate parser_args
    if not isinstance(parser_args, dict):
        raise_type_error_with_signature()
    # Default formatter class
    if "formatter_class" not in parser_args:
        parser_args["formatter_class"] = argparse.RawDescriptionHelpFormatter
    try:
        parser = argparse.ArgumentParser(**parser_args)
    except Exception as e:
        raise Exception(f"Failed to create ArgumentParser: {e}")

    # Validate version
    if not isinstance(version, (str, int)):
        raise_type_error_with_signature()

    # Add version argument
    parser.add_argument(
        "--version",
        action="version",
        version=version if isinstance(version, str) else str(version)
    )

    return parser


def main():
    """Main CLI entry point."""

    parser = create_parser({
        "prog": "sldb",
        "description": "My CLI tool",
    }, "0.0.1")

    args = parser.parse_args()

    print(f"{args=}")


if __name__ == "__main__":
    main()
