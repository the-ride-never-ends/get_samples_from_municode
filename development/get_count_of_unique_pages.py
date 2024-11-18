import os
import re


import networkx as nx
import pandas as pd


from utils.shared.make_sha256_hash import make_sha256_hash

from config.config import OUTPUT_FOLDER
from logger.logger import Logger
logger = Logger(logger_name=__name__)


# def graph_family_tree(family_tree: nx.DiGraph, unique_pages_urls_set: set[list]) -> None:
#     # Draw the graph with penultimate descendants colored red
#     pos = nx.nx_agraph.graphviz_layout(family_tree, prog="dot")
#     plt.figure(figsize=(12, 8))

#     node_colors = [
#         "red" if node in unique_pages_urls_set else "lightblue" for node in family_tree.nodes
#     ]
#     nx.draw(
#         family_tree,
#         pos,
#         with_labels=True,
#         node_size=3000,
#         node_color=node_colors,
#         font_size=10,
#         font_weight="bold",
#     )
#     plt.title("Debugged Family Tree with Penultimate Descendants Highlighted")
#     plt.show()
#     time.sleep(10)
#     return


def get_unique_pages_urls_from_municode_toc(unnested_df: pd.DataFrame) -> int:
    """
    Identify and count penultimate descendants in Municode's nested Tables of Contents.

    This function constructs a directed graph from the input DataFrame,
    identifies ultimate descendants (nodes with no children), and then
    finds their immediate parents (penultimate descendants).

    TODO Generalize it for other kinds of nested objects. 
    It seems pretty useful for things besides the Municode Table of Contents

    Args:
        unnested_df (pd.DataFrame): DataFrame containing 'text' and 'parent_text' columns
                                    representing the hierarchical structure.

    Returns:
        int: The number of penultimate descendants in the family tree.

    Note:
        - Ultimate descendants are nodes with no children.
        - Penultimate descendants are the immediate parents of ultimate descendants.
        - The function logs debug information about ultimate and penultimate descendants.
    """
    # Create a directed graph
    family_tree = nx.DiGraph()

    # Add nodes and edges based on the CSV data
    for row in unnested_df.itertuples(index=False):
        family_tree.add_node(row.text, url=row.url)
        if row.parent_text and row.parent_text != "None" and row.url:
            family_tree.add_edge(row.parent_text, row.text)

    # Identify ultimate descendants (nodes with no children)
    ultimate_descendants = [
        node for node in family_tree.nodes if len(list(family_tree.successors(node))) == 0
    ]
    logger.debug(f"Found Ultimate Descendants (e.g. total Lines in CSV minus 1): {ultimate_descendants}", off=True)

    # Identify penultimate descendants (parents of ultimate descendants)
    penultimate_descendants_set = set()
    for ultimate in ultimate_descendants:
        parents = family_tree.predecessors(ultimate)
        penultimate_descendants_set.update(parents)

    # Get the attributes of the nodes in family_tree if they're in penultimate_descendants_set
    unique_pages_urls_set = {
        family_tree.nodes[node]['url'] for node in penultimate_descendants_set
    }
    logger.debug(f"unique_pages_urls_set\n{unique_pages_urls_set}\npenultimate_node_attributes",f=True,off=True,t=30)

    # num_penultimate_descendants = len(penultimate_descendants_set)
    logger.debug(f"Number of Penultimate Descendants: {len(unique_pages_urls_set)}",f=True,off=True, t=30)

    #graph_family_tree(family_tree, penultimate_descendants_set)

    return unique_pages_urls_set


def _split_city_name_and_gnis_from_filename_postfix(filepath: str, suffix: str) -> tuple[str, int]:
    """
    Extract city name and GNIS from a filepath using a suffix pattern.
    The filename is expected to be in the format: 'cityname_gnisnumber_postfix'.

    Args:
        filepath (str): The full path to the file.
        suffix (str): The expected suffix of the filename.

    Returns:
        tuple[str, int]: A tuple containing the city name (str) and GNIS number (int).

    Raises:
        ValueError: If the city name and GNIS cannot be extracted from the filepath
                    using the given suffix pattern.

    Example:
        filepath = '/path/to/NewYork_1234567_menu_traversal_results_unnested.csv'
        suffix = '_menu_traversal_results_unnested'
        city_name, gnis = _split_city_name_and_gnis_from_filename_postfix(filepath, suffix)
        # Returns: ('NewYork', 1234567)
    """
    filename = os.path.basename(filepath).rsplit(".", 1)[0]  # Remove file extension if present
    filename = filename.removesuffix(suffix) # Remove the suffix.
    pattern = r'(^[a-zA-Z].*)_(\d+)'
    match = re.match(pattern, filename)

    if not match:
        raise ValueError(f"Could not find city name or GNIS in filepath using pattern '{pattern}'\nfilename: {filename}\nfilepath: {filepath}")

    city_name, gnis = match.group(1), match.group(2)
    if not city_name or not gnis:
        raise ValueError(f"Could not extract city name and GNIS from filepath using pattern '{pattern}'\nfilename: {filename}\nfilepath: {filepath}")

    return city_name, int(gnis)


def save_unique_pages_urls_set_to_csv(unique_pages_urls_set: set, filepath: str) -> None:
    """
    Reformat the unique_pages_urls_set set as a pandas DataFrame, save it to CSV.
    The structure of the output CSV matches that in the corresponding MySQL database.


    Args:
        unique_pages_urls_set (set): A set containing the penultimate descendants.

    Returns:
        pd.DataFrame: A DataFrame containing the penultimate descendants URLs.

    Raises:
        ValueError: If the input set is empty.

    Example:
        unique_pages_urls_set = {'example.com/1', 'example.com/2', 'example.com/3'}
        save_penultimate_descendant_to_csv(unique_pages_urls_set)

    Note:
        The CSV file will be saved in the current working directory with the name 'unique_pages_urls_set.csv'.
    """
    # Check if the input set is empty
    if not unique_pages_urls_set:
        raise ValueError("The input set is empty.")

    # Extract city name and GNIS from the filepath
    suffix = "_menu_traversal_results_unnested"
    city_name, gnis = _split_city_name_and_gnis_from_filename_postfix(filepath, suffix)

    # Convert the set to a DataFrame
    output_dict = {
        "url": list(unique_pages_urls_set)
    }
    output_df = pd.DataFrame.from_dict(output_dict)

    # Set the other fields in the dataframe.
    output_df['gnis'] = gnis
    output_df['url_hash'] = output_df.apply(lambda row: make_sha256_hash(row['url'], row['gnis']), axis=1)
    # NOTE Since query_hash is a required field in the MySQL database, we fill it with a placeholder value.
    output_df['query_hash'] = "URL NOT FOUND THROUGH SEARCH QUERY"

    # Reorder the columns to match the MySQL database.
    output_df = output_df[['url_hash', 'query_hash', 'gnis', 'url']]

    logger.debug(f"output_df: {output_df.head()}",f=True,t=30,off=True)

    # Save the DataFrame to a CSV file
    output_folder = os.path.join(OUTPUT_FOLDER, "sql_ready_urls", f"{city_name}_{gnis}_sql_ready_urls.csv")
    output_df.to_csv(output_folder, index=False)

    return


def get_count_of_unique_pages(filepath: str) -> int:
    """
    Get a count of the unique pages for a given Municode library page.
    A unique page is defined as a URL that is at the 2nd to last level of a parent hierarchy.

    Args:
        filepath: A string representing the path to the CSV file.

    Returns:
        A dictionary mapping URLs to their penultimate level descendant counts.

    Example:
        if __name__ == "__main__":
            counts = get_count_of_unique_pages('example_123456789_postfix.csv')
            print("\nPenultimate Level Counts:")
            for url, count in sorted(counts.items()):
                print(f"{url}: {count}")
    """
    unnested_df = pd.read_csv(filepath)

    unique_pages_urls_set = get_unique_pages_urls_from_municode_toc(unnested_df)

    output_folder = os.path.join(OUTPUT_FOLDER, "sql_ready_urls")
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    save_unique_pages_urls_set_to_csv(unique_pages_urls_set, filepath)

    return len(unique_pages_urls_set)

