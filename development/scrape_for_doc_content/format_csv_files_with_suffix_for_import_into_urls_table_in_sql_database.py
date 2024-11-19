import os


import pandas as pd


from .split_city_name_and_gnis_from_filename_suffix import split_city_name_and_gnis_from_filename_suffix
from utils.shared.make_sha256_hash import make_sha256_hash


from config.config import OUTPUT_FOLDER
from logger.logger import Logger
logger = Logger(logger_name=__name__)


def _input_container_to_pandas_df(
        input_container: set|pd.DataFrame,
        suffix: str
        ) -> pd.DataFrame:
    
    if isinstance(input_container, set):
        output_dict = {
            "url": list(input_container)
        }
        output_df = pd.DataFrame.from_dict(output_dict)
    elif isinstance(input_container, pd.DataFrame):
        if suffix == "_menu_traversal_results_unnested":
            columns_to_keep = ['url', 'depth']
            output_df = input_container[columns_to_keep]
        else:
            raise ValueError(f"input_container is a pandas DataFrame, but the suffix is not '_menu_traversal_results_unnested'. The suffix is {suffix}.")
    else:
        raise ValueError(f"input_container is not a set or pandas DataFrame, but a {type(input_container)}")
    return output_df



def format_csv_files_with_suffix_for_import_into_urls_table_in_sql_database(
        input_container: set|list|tuple|pd.DataFrame|dict|frozenset,
        filepath: str, 
        suffix: str,
        output_suffix: str="sql_ready_urls") -> None:
    """
    Reformat the input_container set as a pandas DataFrame, save it to CSV.
    The structure of the output CSV matches that in the corresponding MySQL database.


    Args:
        input_container (set): A set containing the penultimate descendants.

    Returns:
        pd.DataFrame: A DataFrame containing the penultimate descendants URLs.

    Raises:
        ValueError: If the input set is empty.

    Example:
        input_container = {'example.com/1', 'example.com/2', 'example.com/3'}
        save_penultimate_descendant_to_csv(input_container)

    Note:
        The CSV file will be saved in the current working directory with the name 'input_container.csv'.
    """
    # Check if the input set is empty
    if not input_container:
        raise ValueError("The input input_container is empty.")

    # Extract city name and GNIS from the filepath
    city_name, gnis = split_city_name_and_gnis_from_filename_suffix(filepath, suffix)

    # Convert input_container to a DataFrame
    output_df = _input_container_to_pandas_df(input_container, suffix)

    # Set the other fields in the dataframe.
    output_df['gnis'] = gnis
    output_df['url_hash'] = output_df.apply(lambda row: make_sha256_hash(row['url'], row['gnis']), axis=1)
    # NOTE Since query_hash is a required field in the MySQL database, we fill it with a placeholder value.
    output_df['query_hash'] = "URL NOT FOUND THROUGH SEARCH QUERY"
    output_df['municode_url_depth'] = output_df['node_depth'] if 'node_depth' in output_df.columns else None

    # Reorder the columns to match the MySQL database.
    output_df = output_df[['url_hash', 'query_hash', 'gnis', 'url', 'municode_url_depth']]

    logger.debug(f"output_df: {output_df.head()}",f=True,t=30,off=True)

    # Save the DataFrame to a CSV file
    output_folder = os.path.join(OUTPUT_FOLDER, output_suffix, f"{city_name}_{gnis}_{output_suffix}.csv")
    output_df.to_csv(output_folder, index=False)

    return output_folder