import os
import xml.etree.ElementTree as ET

from .exceptions import raise_auto_arg_type_error

SUPPORTED_REC_ATTRS = {
    'id': 'external_id',
    'model': 'model'
}

def get_data_from_file(file_path: str) -> list[dict]:
    """
        Load data from a file and return it as a standard list of dictionaries.
    """
    # Check filepath
    if not isinstance(file_path, str):
        raise_auto_arg_type_error(file_path)
    if not file_path:
        raise ValueError("File path is empty.")
    if not file_path.endswith(".xml"):
        raise ValueError("File path must end with '.xml'")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    # Load xml file
    tree = ET.parse(file_path)
    # Parse xml
    root = tree.getroot()
    if root.tag != "data":
        raise ValueError(f"Invalid XML root element: {root.tag}. Expected 'data'.")
    # Get all elements that are direct children of <data> (should all be <record>)
    records = root.findall("./")
    datas = []
    for record in records:
        # Raise if not a <record> element
        if record.tag != "record":
            raise ValueError(f"Invalid XML element: {record.tag}. Expected 'record'.")
        # Assemble record metadata
        record_metadata = {}
        for attr_name, attr_value in record.items():
            if attr_name not in SUPPORTED_REC_ATTRS:
                raise ValueError(f"Unsupported record attribute: {attr_name}")
            record_metadata[SUPPORTED_REC_ATTRS[attr_name]] = attr_value
        # Assemble record field data
        field_data = {field.tag: field.text for field in record}
        # Assemble all record data
        record_data = {
            'metadata': record_metadata,
            'vals': field_data
        }
        datas.append(record_data)
    return datas
