import os


import pandas as pd


from config.config import INPUT_FOLDER


def load_from_csv_via_pandas(csv_file_path: str) -> pd.DataFrame:
    """
    Load CSV file using pandas, getting header names from the first line
    """
    return pd.read_csv(os.path.join(INPUT_FOLDER, csv_file_path))
