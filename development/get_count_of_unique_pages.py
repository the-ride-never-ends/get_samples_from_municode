from collections import defaultdict
import time

import tqdm

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd


from logger.logger import Logger
logger = Logger(logger_name=__name__)



def build_hierarchy(df: pd.DataFrame) -> dict[str, set[str]]:
    """
    Builds a proper parent-child hierarchy.
    """
    # Create direct parent-child relationships
    children = defaultdict(set)
    for row in df.itertuples():
        if row.parent_text:  # Only add if there's a parent
            children[row.parent_text].add(row.text)
    return children


def get_depth(text: str, df: pd.DataFrame) -> int:
    """Get the depth of a node from the DataFrame."""
    matches = df[df['text'] == text]['depth']
    return matches.iloc[0] if not matches.empty else 0


def get_descendants_by_depth(node: str, depth_target: int, current_depth: int, 
                           children: dict[str, set[str]], df: pd.DataFrame, 
                           cache: dict[str, set[str]]) -> set[str]:
    """
    Gets descendants at a specific depth using child relationships.
    Uses caching to avoid recomputing.
    """
    cache_key = f"{node}:{depth_target}"
    if cache_key in cache:
        return cache[cache_key]

    if current_depth == depth_target:
        result = {node}
    elif current_depth > depth_target:
        result = set()
    else:
        result = set()
        for child in children.get(node, set()):
            child_depth = get_depth(child, df)
            result.update(get_descendants_by_depth(
                child, depth_target, child_depth,
                children, df, cache
            ))

    cache[cache_key] = result
    return result


# def graph_family_tree(family_tree: nx.DiGraph, penultimate_descendants: set[list]) -> None:
#     # Draw the graph with penultimate descendants colored red
#     pos = nx.nx_agraph.graphviz_layout(family_tree, prog="dot")
#     plt.figure(figsize=(12, 8))

#     node_colors = [
#         "red" if node in penultimate_descendants else "lightblue" for node in family_tree.nodes
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


def get_penultimate_descendants(unnested_df: pd.DataFrame) -> int:
    """
    Debug, identify, and print penultimate descendants in the family tree.

    :param csv_data: DataFrame containing 'name', 'parent', and 'depth' columns
    """
    # Create a directed graph
    family_tree = nx.DiGraph()

    # Add nodes and edges based on the CSV data
    for row in unnested_df.itertuples(index=False):
        family_tree.add_node(row.text)
        if row.parent_text and row.parent_text != "None":
            family_tree.add_edge(row.parent_text, row.text)

    # Identify ultimate descendants (nodes with no children)
    ultimate_descendants = [
        node for node in family_tree.nodes if len(list(family_tree.successors(node))) == 0
    ]
    logger.debug(f"Ultimate Descendants (e.g. total Lines in CSV minus 1): {ultimate_descendants}", off=True)

    # Identify penultimate descendants (parents of ultimate descendants)
    penultimate_descendants = set()
    for ultimate in ultimate_descendants:
        parents = list(family_tree.predecessors(ultimate))
        penultimate_descendants.update(parents)

    # Print penultimate descendants to the console
    logger.debug(f"penultimate_descendants: {penultimate_descendants}",f=True,off=True)

    num_penultimate_descendants = len(penultimate_descendants)
    logger.info(f"Number of Penultimate Descendants: {num_penultimate_descendants}",f=True,off=True)

    #graph_family_tree(family_tree, penultimate_descendants)

    return num_penultimate_descendants


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
            counts = analyze_url_hierarchy('paste.txt')
            print("\nPenultimate Level Counts:")
            for url, count in sorted(counts.items()):
                print(f"{url}: {count}")
    """
    unnested_df = pd.read_csv(filepath)

    num_penultimate_descendants = get_penultimate_descendants(unnested_df)

    return num_penultimate_descendants
