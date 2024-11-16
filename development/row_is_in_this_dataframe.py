from typing import Any


import pandas as pd


def row_is_in_this_dataframe(value: Any, column: str, df: pd.DataFrame) -> bool:
    """
    Check if a value exists in a specific column of a pandas DataFrame.

    Args:
        value (Any): The value to search for.
        column (str): The column name to search in.
        df (pd.DataFrame): The pandas DataFrame to search.

    Returns:
        bool: True if the value is found, False otherwise.
    """
    return value in df[column].values