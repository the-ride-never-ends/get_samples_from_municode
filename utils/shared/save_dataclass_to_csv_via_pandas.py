from dataclasses import fields, is_dataclass
import os
from typing import Any


import pandas as pd


from config.config import OUTPUT_FOLDER


def _convert_dataclass_to_dict(obj: Any) -> dict:
    """
    Recursively convert a dataclass object to a dictionary.
    
    Args:
        obj: The object to convert. Can be a dataclass or any other type.
    
    Returns:
        A dictionary representation of the object.
    """
    if not is_dataclass(obj):
        return obj

    result = {}
    try:
        for field in fields(obj):
            value = _value = getattr(obj, field.name)
            converted = False

            # Recursively convert nested dataclasses
            if is_dataclass(_value):
                conv_value = _convert_dataclass_to_dict(_value)
                converted = True
            # Handle lists of dataclasses
            elif isinstance(_value, list):
                conv_value = [_convert_dataclass_to_dict(item) if is_dataclass(item) else item for item in _value]
                converted = True
            # Handle dictionaries that might contain dataclasses
            elif isinstance(_value, dict):
                conv_value = {k: _convert_dataclass_to_dict(v) if is_dataclass(v) else v for k, v in _value.items() }
                converted = True

            elif isinstance(_value, set):
                conv_value = [_convert_dataclass_to_dict(item) if is_dataclass(item) else item for item in _value]
                converted = True
            
            result[field.name] = value if not converted else conv_value
    except Exception as e:
        print(f"Error converting dataclass to dict: {e}")
        raise e

    return result


def _get_csv_rows_from_dataclass_values(dataclass: Any | list[Any] | dict[str, Any]) -> list[dict[str, Any]] | list[str]:
    """
    Unpack a dataclass and its children or an iterable of dataclasses into a list of dictionaries.
    
    Args:
        dataclass: A dataclass instance, list of dataclasses, or dictionary of dataclasses
        
    Returns:
        List of dictionaries representing rows for CSV conversion
        
    Raises:
        ValueError: If input type is not supported
    """
    # Type check, then convert the dataclass to a 2D list of dictionaries.
    if isinstance(dataclass, list):
        if not dataclass:
            raise ValueError("Empty list provided")
        if not is_dataclass(dataclass[0]):
            raise ValueError(f"List items must be dataclasses, got {type(dataclass[0]).__name__}")
        return [_convert_dataclass_to_dict(obj) for obj in dataclass]

    elif is_dataclass(dataclass):
        return [_convert_dataclass_to_dict(dataclass)]

    if isinstance(dataclass, dict):
        return [{k: _convert_dataclass_to_dict(v) if is_dataclass(v) else v 
                for k, v in dataclass.items()}]
    else:
        raise ValueError(f"'{dataclass.__class__.__name__}' is not supported.\nPlease provide a dataclass, list of dataclasses, or a dictionary of dataclasses.")


def _get_csv_headers_from_dataclass_keys(dataclass: Any|list[Any]|dict[str, Any]) -> list[str]:
    """
    Extract column headers from a dataclass.
    
    Args:
        dataclass: A dataclass instance
        
    Returns:
        List of header names
        
    Raises:
        ValueError: If input is not a dataclass
    """
    if not is_dataclass(dataclass):
        raise ValueError(f"Expected a dataclass, got {type(dataclass).__name__}")

    headers_dict = _convert_dataclass_to_dict(dataclass)
    return list(headers_dict.keys())


def save_dataclass_to_csv_via_pandas(dataclass: Any|list[Any]|dict[str, Any], 
                                filename: str = "output.csv", 
                                index: bool = False, 
                                encoding: str = "utf-8", 
                                return_df: bool=False
                                ) -> None | pd.DataFrame:
    """
    Convert a dataclass or list of dataclasses to a CSV file using pandas.

    This function takes a dataclass instance, a list of dataclass instances, or a dictionary of dataclasses,
    converts them to a pandas DataFrame, and then saves the DataFrame as a CSV file. 
    The CSV file is saved in the directory specified by the OUTPUT_FOLDER constant.

    Args:
        dataclass (Any | list[Any] | dict[str, Any]): A dataclass instance, list of dataclass instances, or dictionary of dataclasses to be converted.
        filename (str, optional): The name of the output CSV file. Defaults to "output.csv".
        index (bool, optional): Whether to include the index in the CSV file. Defaults to False.
        encoding (str, optional): The encoding to use when writing the CSV file. Defaults to "utf-8".
        return_df (bool, optional): Whether to return the pandas DataFrame. Defaults to False.

    Returns:
        None | pd.DataFrame: If return_df is True, returns the pandas DataFrame. Otherwise, returns None.

    Raises:
        ValueError: If the input dataclass is not of the expected type.
    """

    filepath = os.path.join(OUTPUT_FOLDER, filename)
    print("Converting dataclass to pandas DataFrame...")

    rows: list[dict] = _get_csv_rows_from_dataclass_values(dataclass)

    dataclass = dataclass[0] if isinstance(dataclass, list) else dataclass
    headers: list[str] = _get_csv_headers_from_dataclass_keys(dataclass)

    df = pd.DataFrame.from_records(rows, columns=headers)

    print(f"Saving DataFrame CSV to '{filepath}'...")
    df.to_csv(filepath, index=index, encoding=encoding)
    print("CSV saved successfully.")
    return df if return_df else None
