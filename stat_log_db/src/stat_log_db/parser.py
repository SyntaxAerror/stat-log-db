import argparse

from .exceptions import raise_auto_arg_type_error


def create_parser(parser_args: dict, version: str | int = "0.0.1") -> argparse.ArgumentParser:
    """Create the main argument parser."""
    # Validate parser_args
    if not isinstance(parser_args, dict):
        raise_auto_arg_type_error("parser_args")
    # Default formatter class
    if "formatter_class" not in parser_args:
        parser_args["formatter_class"] = argparse.RawDescriptionHelpFormatter
    try:
        parser = argparse.ArgumentParser(**parser_args)
    except Exception as e:
        raise Exception(f"Failed to create ArgumentParser: {e}")

    # Validate version
    if not isinstance(version, (str, int)):
        raise_auto_arg_type_error("version")

    # Add version argument
    parser.add_argument(
        "--version",
        action="version",
        version=version if isinstance(version, str) else str(version)
    )

    return parser
