import inspect
import os
from utils.shared.sanitize_filename import sanitize_filename
from config.config import OUTPUT_FOLDER
def make_path_from_function_name(filename: str="output.txt") -> str:
    """
    Make a path from the name of the function that called this function.
    Return a path ../outputs/<calling_function_name>/<calling_function_name>_<filename>.
    If this path save the root doesn't exist, it will make it.
    """
    # Get the frame of the calling function
    calling_frame = inspect.currentframe().f_back
    
    # Get the name of the calling function
    calling_function_name = calling_frame.f_code.co_name

    # Make the folder if it doesn't exist.
    output_folder = os.path.join(OUTPUT_FOLDER, calling_function_name)
    if not os.path.exists(output_folder):
        print(f"Creating output folder: {output_folder}")
        os.mkdir(output_folder)

    # Construct the filename from the function's name and filename.
    filename = filename or sanitize_filename(calling_function_name)
    filename += f"_{filename}" if filename else ""

    return os.path.join(output_folder, filename)
