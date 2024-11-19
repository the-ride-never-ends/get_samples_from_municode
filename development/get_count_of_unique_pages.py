import os
import re


import networkx as nx
import pandas as pd


from utils.shared.make_sha256_hash import make_sha256_hash
from .scrape_for_doc_content.split_city_name_and_gnis_from_filename_suffix import split_city_name_and_gnis_from_filename_suffix
from .scrape_for_doc_content.format_csv_files_with_suffix_for_import_into_urls_table_in_sql_database import (
    format_csv_files_with_suffix_for_import_into_urls_table_in_sql_database
)

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
    suffix = "_menu_traversal_results_unnested"
    unnested_df = pd.read_csv(filepath)

    unique_pages_urls_set = get_unique_pages_urls_from_municode_toc(unnested_df)

    output_folder = os.path.join(OUTPUT_FOLDER, "sql_ready_urls")
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    format_csv_files_with_suffix_for_import_into_urls_table_in_sql_database(unique_pages_urls_set, filepath, suffix)

    return len(unique_pages_urls_set)

