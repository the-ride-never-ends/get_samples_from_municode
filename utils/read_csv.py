import csv
from typing import Iterable, Callable


def read_csv(file_path: str, skip_headers: bool=True, row_func: Callable=None, row_func_kwargs: dict=None, **kwargs) -> list[Iterable]:
    """
    Read a CSV file and return its contents as a list of iterables.

    This function reads a CSV file, processes its contents, and returns the data as a list of iterables.
    It can optionally skip the header row and apply a custom function to each row.

    Args:
        file_path (str): The path to the CSV file to be read.
        skip_headers (bool, optional): Whether to skip the first row (headers) of the CSV file. Defaults to True.
        row_func (Callable, optional): A function to apply to each row before adding it to the output. Defaults to None.
        **kwargs: Additional keyword arguments to pass to the open() function. Defaults to {"mode": 'r', 'newline':''}

    Returns:
        list[Iterable]: A list of Iterables (e.g., list, tuple), where each iterable represents a row from the CSV file.
                        Empty rows are skipped.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        Exception: For any other errors that occur during file reading or processing.

    Example:
        >>> data = read_csv('example.csv', skip_headers=True, row_func=lambda x: [int(i) for i in x])
        >>> print(data)
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    """
    try:

        if kwargs is None: # Default kwargs if not provided
            kwargs = {"mode": 'r', 'newline':''}

        with open(file_path, **kwargs) as f:
            reader = csv.reader(f)

            if skip_headers:
                next(reader, None)

            if row_func is not None:
                if row_func_kwargs is None: # Default kwargs if not provided
                    output = [row_func(row) if row_func else row for row in reader if row]
                else:
                    output = [row_func(row, **row_func_kwargs) if row_func else row for row in reader if row]
            else:
                output = [row for row in reader if row]

    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return output
