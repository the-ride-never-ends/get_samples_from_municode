import pandas as pd
from collections import defaultdict
from typing import Dict, Set


from logger.logger import Logger
logger = Logger(logger_name=__name__)


def build_hierarchy(df: pd.DataFrame) -> Dict[str, Set[str]]:
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
                           children: Dict[str, Set[str]], df: pd.DataFrame, 
                           cache: Dict[str, Set[str]]) -> Set[str]:
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

def get_penultimate_counts(df: pd.DataFrame) -> Dict[str, int]:
    """
    Returns counts of descendants at penultimate levels for each URL.
    """
    # Build proper hierarchy
    children = build_hierarchy(df)
    cache = {}
    
    # Get max depth for each root
    results = {}
    
    for row in df.itertuples():
        node = row.text
        node_depth = row.depth
        
        # Find maximum depth of descendants
        max_descendant_depth = node_depth
        for potential_depth in range(node_depth + 1, df['depth'].max() + 1):
            descendants = get_descendants_by_depth(
                node, potential_depth, node_depth,
                children, df, cache
            )
            if descendants:
                max_descendant_depth = potential_depth
        
        # If node has descendants beyond its depth,
        # count those at penultimate level
        if max_descendant_depth > node_depth:
            penultimate_depth = max_descendant_depth - 1
            penultimate_descendants = get_descendants_by_depth(
                node, penultimate_depth, node_depth,
                children, df, cache
            )
            if penultimate_descendants:
                results[node] = len(penultimate_descendants)
    
    return results


def get_level_zero_counts(df: pd.DataFrame) -> Dict[str, int]:
    for row in df.itertuples():
        if row.depth == 0:
            

def get_count_of_unique_pages(filepath: str) -> dict[str, int]:
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
    data = pd.read_csv(filepath)
    count_dicts = get_penultimate_counts(data)
    logger.info(f"Count dicts: {count_dicts}", t=30)
    count_tuples = list(count_dicts.values())
    logger.info(f"Count tuples: {count_tuples}", t=30)

    output_count = [tup for tup in count_tuples]
    output_count = sum(count_tuples)

    return output_count
