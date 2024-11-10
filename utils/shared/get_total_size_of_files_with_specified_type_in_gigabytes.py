import os


def get_total_size_of_files_with_specified_type_in_gigabytes(directory: str, filetype: str = ".html") -> float:
    """
    Calculate the total size of files with a specified type in a given directory.

    This function iterates through all files in the specified directory, 
    sums up the sizes of files with the given filetype, and returns the 
    total size in gigabytes. It also writes this information to a text file.

    Args:
        directory (str): The path to the directory containing the files.
        filetype (str, optional): The file extension to filter by. Defaults to ".html".

    Returns:
        float: The total size of matching files in gigabytes.

    Side effects:
        Creates or overwrites a file named "total_size.txt" with the calculated size information.
    """
    total_size = 0
    for filename in os.listdir(directory):
        if filename.endswith(f".{filetype}"):
            file_path = os.path.join(directory, filename)
            total_size += os.path.getsize(file_path)

    # Convert bytes to gigabytes
    total_size_in_gbs = total_size / 1024 / 1024 / 1024 # byte -> kilobyte -> megabyte -> gigabyte

    # Write the total size to a text file
    with open("total_size.txt", "w") as f:
        f.write(f"Total size of {filetype} files in the directory: {total_size_in_gbs:.2f} GB")

    return total_size_in_gbs