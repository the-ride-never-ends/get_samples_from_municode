import os
from typing import NamedTuple


import pandas as pd


from config.config import INPUT_FOLDER


def append_pandas_row_to_csv(row: NamedTuple, filename: str) -> None:
    """
    Append a single row from a pandas NamedTuple to a CSV file using pandas.
    
    Args:
        row (NamedTuple): The row to append.
        filename (str): The name of the CSV file.
    """
    df = pd.DataFrame(
        {column: [getattr(row, column)] for column in row._fields if column != 'Index'}
    )
    df.to_csv(os.path.join(INPUT_FOLDER, filename), index=False, mode='a', header=False)
    print(f"Appended row to {filename}.")
    return

