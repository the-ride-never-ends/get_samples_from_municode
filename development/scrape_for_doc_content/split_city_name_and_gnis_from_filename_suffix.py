import os
import re


def split_city_name_and_gnis_from_filename_suffix(filepath: str, suffix: str) -> tuple[str, int]:
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
        city_name, gnis = split_city_name_and_gnis_from_filename_suffix(filepath, suffix)
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