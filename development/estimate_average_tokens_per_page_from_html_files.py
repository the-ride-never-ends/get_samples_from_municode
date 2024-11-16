import os


from bs4 import BeautifulSoup
import tiktoken as tk
import tqdm


from config.config import OUTPUT_FOLDER
from logger.logger import Logger
logger = Logger(logger_name=__name__)


def _get_total_number_of_tokens_for_html_files_in(
    dir_path: str,
    class_: str,
    html_files: list[str]
    ) -> list[int]:
    """
    Calculate the total number of tokens for HTML files in a directory.

    This function processes a list of HTML files, extracts content from elements
    with a specified class, and calculates the token count for each file.

    Args:
        dir_path (str): The path to the directory containing HTML files.
        class_ (str): The CSS class name to search for in the HTML files.
        html_files (list[str]): A list of HTML file names to process.

    Returns:
        list[int]: A list of total token counts, one for each processed HTML file.

    Note:
        - Uses the GPT-4 tokenizer for encoding.
        - Skips files where no elements with the specified class are found.
        - Logs information about each processed file (can be turned off).
    """

    encoding = tk.encoding_for_model("gpt-4o")
    total_tokens = []

    for file in tqdm.tqdm(html_files, desc="Processing HTML files", unit="file"):

        with open(os.path.join(dir_path, file), "r", encoding="utf-8") as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, "html.parser")

        # Find all elements with class 'chunk-content-wrapper'
        chunk_content_wrappers = soup.find_all(class_=class_)
        if chunk_content_wrappers == 0:
            # print(f"No text under class '{class_}' found in HTML. Skipping...")
            continue

        # Get the token count for each text.
        # NOTE We don't need the tokens themselves, just how many of them there are.
        html_chunk_content = [
            len(encoding.encode(wrapper.get_text(strip=True))) for wrapper in chunk_content_wrappers
        ]

        # Calculate total number of tokens and
        # average tokens per chunk for this HTML file.
        total_tokens_in_file = sum(html_chunk_content)
        total_num_wrappers = len(chunk_content_wrappers)
        try:
            average_tokens_per_chunk = total_tokens_in_file / total_num_wrappers
        except ZeroDivisionError:
            average_tokens_per_chunk = 0

        logger.info(f"""
        HTML File: {file}
        Total chunks: {total_num_wrappers:,}
        Total tokens: {total_tokens_in_file:,}
        Average tokens per chunk: {average_tokens_per_chunk:,}
        """,f=True,off=True)

        total_tokens.append(total_tokens_in_file)
    return total_tokens

def estimate_average_tokens_per_page_from_html_files(class_: str = "chunk-content-wrapper") -> float:
    """
    Estimate the average number of tokens per page from HTML files in a specific directory.

    Returns:
        float: The average number of tokens per HTML file. Returns 100 if no HTML files are found.

    Note:
        - The function looks for HTML files in the directory specified by OUTPUT_FOLDER/scrape_municode_library_page.
        - It searches for elements with the class 'chunk-content-wrapper' within each HTML file.
        - The token count is based on the GPT-4 tokenizer.
        - Logging is used to provide information about the process and results.
    """
    # Define constants
    dir_path = os.path.join(OUTPUT_FOLDER, "scrape_municode_library_page")

    # Get the html files in the directory and how many of them there are.
    html_files = [file for file in os.listdir(dir_path) if file.endswith(".html")]
    if html_files == 0:
        logger.error(f"No HTML files found in directory '{dir_path}'. Returning 1000 as default...")
        return 100

    # Get the total tokens for the HTML files in the directory.
    total_tokens= _get_total_number_of_tokens_for_html_files_in(dir_path, class_, html_files)

    average_per_file = sum(total_tokens) / len(html_files) if html_files else 0

    logger.info(f"""
    Total HTML Files: {len(html_files):,}
    Total tokens: {sum(total_tokens):,}
    Average tokens within
    Average tokens per file: {average_per_file:,}
    """,f=True)

    return average_per_file
