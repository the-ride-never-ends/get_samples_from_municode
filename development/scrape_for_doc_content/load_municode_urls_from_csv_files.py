import os


import pandas as pd
from tqdm import tqdm


from development.scrape_for_doc_content.split_city_name_and_gnis_from_filename_suffix import (
    split_city_name_and_gnis_from_filename_suffix
)

from logger.logger import Logger
logger = Logger(logger_name=__name__)


def load_municode_urls_from_csv_files(folder_path: str, 
                                    output_suffix: str,
                                    url_hash_df: pd.DataFrame = None,
                                    gnis_df: pd.DataFrame = None,
                                    urls_df_list: list[pd.DataFrame] = None
                                    ) -> list[pd.DataFrame]:

    # Check if the kwarg arguments are provided.
    if url_hash_df is None or gnis_df is None or urls_df_list is None:
        raise ValueError("url_hash_df, gnis_df, and urls_df_list must be provided as arguments.")

    # Get the paths for the CSV files.
    path_list = [
        path for path in os.listdir(folder_path) if path.endswith(".csv") and output_suffix in path
    ]

    for path in tqdm(path_list, desc="Loading URLs from available CSV files"):
        # Get the CSV's GNIS and load in the file.
        _, gnis = split_city_name_and_gnis_from_filename_suffix(path, output_suffix)
        csv_urls_df = pd.read_csv(os.path.join(folder_path, path))

        # Remove rows from csv_urls_df where the url_hash is present in url_hash_df
        if gnis in url_hash_df['gnis']:
            csv_urls_df = csv_urls_df[~csv_urls_df['url_hash'].isin(url_hash_df['url_hash'])]
        logger.debug(f"csv_urls_df: {csv_urls_df.head()}", f=True, t=30)

        # Add the paired-down csv_urls_df to urls_df_list
        urls_df_list.append(csv_urls_df)

        # Remove the processed GNIS from gnis_df
        gnis_df = gnis_df[gnis_df["gnis"] != gnis]
    return urls_df_list
