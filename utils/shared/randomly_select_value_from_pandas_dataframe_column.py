import random
from typing import Any


import pandas as pd


def randomly_select_value_from_pandas_dataframe_column(column_name: str, df: pd.DataFrame, seed: int=69) -> Any:
    """
    Randomly selects a value from a specified column in a pandas DataFrame.

    Args:
        column_name (str): The name of the column to select from.
        df (pd.DataFrame): The pandas DataFrame containing the data.
        seed (int, optional): Seed for random number generation. Defaults to 69.

    Returns:
        A randomly selected value from the specified column.

    Raises:
        KeyError: If the specified column_name is not found in the DataFrame.
        IndexError: If the DataFrame is empty.
    """
    # Check if the column name is in the dataframe
    if column_name not in df.columns:
        raise KeyError(f"Column '{column_name}' not found in the DataFrame.")

    # Check if the dataframe is empty.
    if df.empty:
        raise IndexError("The DataFrame is empty.")

    # Set the seed
    random.seed(seed)

    # Randomly selected the index.
    selected_index = random.randint(0, len(df) - 1)

    # Use the index to get the row's value in the specified column
    selected_value = df.loc[selected_index, column_name]

    return selected_value