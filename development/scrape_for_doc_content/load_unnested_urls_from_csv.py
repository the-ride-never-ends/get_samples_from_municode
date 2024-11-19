import os


import pandas as pd


from development.scrape_for_doc_content.split_city_name_and_gnis_from_filename_suffix import (
    split_city_name_and_gnis_from_filename_suffix
)
from config.config import OUTPUT_FOLDER


def load_unnested_urls_from_csv(suffix: str,
                                 output_suffix: str,
                                 directory: str = None,
                                 ) -> list[tuple[str, pd.DataFrame]]:
    """
    Load unnested URLs from CSV files with a specific suffix, excluding already processed files.

    This function searches for CSV files in a specified directory (or the default OUTPUT_FOLDER)
    that end with the given suffix. It reads each file into a DataFrame, but excludes files
    that have already been processed and exist in the output_suffix directory.
    NOTE: CSV file names are assumed to be in the format "<city_name>_<gnis>_<suffix>.csv"

    Args:
        suffix (str): The file suffix to search for (e.g., "_menu_traversal_results_unnested").
        output_suffix (str): The directory name where processed files are stored.
        directory (str, optional): The directory in the output folder to search in. If None, uses the default OUTPUT_FOLDER.

    Returns:
        list[tuple[str, pd.DataFrame]]: A list of tuples, each containing:
            - file_path (str): The full path of the CSV file.
            - unnested_urls_df (pd.DataFrame): The DataFrame containing the unnested URLs.
    """
    folder_path = os.path.join(OUTPUT_FOLDER) if directory is None else os.path.join(OUTPUT_FOLDER, directory)

    already_sql_ready_files = [ # Get the gnis from each file in the output_suffix folder
        split_city_name_and_gnis_from_filename_suffix(file)[1] for file in os.listdir(output_suffix) if file.endswith(".csv")
    ]

    unnested_urls_df_tuple_list = [
        (file_path, pd.read_csv(file_path),) 
        for file_path in os.listdir(folder_path) 
        if suffix in os.path.basename(file_path) # Check if the file name contains the suffix
        and file_path.endswith(".csv") # Check if the file is a CSV file
        and split_city_name_and_gnis_from_filename_suffix(file_path)[1] not in already_sql_ready_files # Check if the file has already been processed
    ]

    return folder_path, unnested_urls_df_tuple_list