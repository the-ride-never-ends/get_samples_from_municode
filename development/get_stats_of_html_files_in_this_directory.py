import os


from logger.logger import Logger
logger = Logger(logger_name=__name__)


def get_stats_of_html_files_in_this_directory(directory: str, filetype: str = "html") -> float:
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
    total_size_in_folder = 0
    total_number_of_files = 0
    for filename in os.listdir(directory):
        if filename.endswith(filetype):
            file_path = os.path.join(directory, filename)
            total_size_in_folder += os.path.getsize(file_path)
            total_number_of_files += 1

    sizes = [
        total_size_in_folder/1028**i for i in range(4)
    ]
    average_file_size_in_gigabytes = sizes[3] / total_number_of_files

    # Convert bytes to gigabytes
    logger.info(f"""
    Total number of {filetype} files in the directory: {total_number_of_files}
    Total size of {filetype} files in the directory
    - {sizes[0]} bytes
    - {sizes[1]} kilobytes
    - {sizes[2]} megabytes
    - {sizes[3]} gigabytes
    Average size of html files in the directory: {average_file_size_in_gigabytes} gigabytes)
    """,f=True)

    # Write the total size to a text file
    with open("total_size.txt", "w") as f:
        f.write(f"Total size of {filetype} files in the directory: {sizes[3]:.10f} gigabytes")

    return average_file_size_in_gigabytes