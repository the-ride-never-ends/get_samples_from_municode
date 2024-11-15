
import json
import re
from typing import NamedTuple


import ast
import pandas as pd


def _flatten_children(row: NamedTuple) -> list[dict]:
    """
    Recursively flatten the children column of a dataframe row.
    
    Args:
        row: A pandas namedtuple row containing a 'children' column with nested data
        
    Returns:
        List of dictionaries containing flattened data
    """
    flattened = []
    
    # Add the root node (depth 0)
    root_record = {
        'text': row.text,
        'parent_text': None,  # Root nodes have no parent
        'metadata': row.metadata,  # Preserve original metadata
        'url': row.url,
        'node_id': row.node_id,
        'depth': 0  # Explicitly set depth to 0 for root nodes
    }
    flattened.append(root_record)


    def _flatten(item: dict, parent_text: str = None):
        # Create a record for the current item
        record = {
            'text': item['text'],
            'parent_text': parent_text,
            'metadata': json.dumps(item.get('metadata', {})),
            'url': item.get('url', ''),
            'node_id': item.get('node_id', ''),
            'depth': item.get('depth', '')
        }
        flattened.append(record)
        
        # Recursively process children
        for child in item.get('children', []):
            _flatten(child, parent_text=item['text'])
    
    # Convert string representation of list to Python object
    if isinstance(row.children, str):
        try:
            children = ast.literal_eval(row.children)
        except:
            children = []
    else:
        children = row.children
    
    # Process each top-level child
    for child in children:
        _flatten(child, parent_text=row.text)
    
    return flattened

def unnest_csv(input_file: str | pd.DataFrame, output_file):
    """
    Un-nest a CSV file with a nested 'children' column.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file

    Example:
    >>> # Example usage
        if __name__ == "__main__":
            input_file = "nested.csv"
            output_file = "unnested.csv"
            
            result = unnest_csv(input_file, output_file)
            print(f"Successfully unnested CSV. First few rows of result:")
            print(result.head())
    """
    if isinstance(input_file, str):
        df = pd.read_csv(input_file)
    else: # If it's a dataframe, just rename it to df for consistency.
        df = input_file

    # Flatten the nested structure
    flattened_data = []
    for row in df.itertuples():
        if not isinstance(row, tuple) or not hasattr(row, '_fields'):
            raise TypeError(f"Row is not a NamedTuple but {type(row)}")

        flattened_data.extend(_flatten_children(row))

    # Create new dataframe from flattened data
    result_df = pd.DataFrame(flattened_data)

    # Regex the text column to remove large spaces leftover from the scraping process.
    result_df['text'] = result_df['text'].apply(lambda text: re.sub(r'\s{2,}', ' ', text) if isinstance(text, str) else text)

    # Sort by depth to ensure proper hierarchy in output
    result_df = result_df.sort_values('depth').reset_index(drop=True)

    # Save to CSV
    result_df.to_csv(output_file, index=False)

    return result_df

