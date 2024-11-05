
import asyncio
import os
import sys
import time


import pandas as pd
from playwright.async_api import async_playwright

from utils.shared.next_step import next_step
from utils.shared.sanitize_filename import sanitize_filename

from config.config import *

from logger.logger import Logger
logger = Logger(logger_name=__name__)


def get_urls_from_csv_via_pandas(csv_file_path: str) -> pd.DataFrame:
    """
    Step 1. Get 30 URLs from CSV
    """
    # Load in CSV file
    return pd.read_csv(csv_file_path)


def go_to_url(url: str, pw_instance) -> None:
    """
    Step 2. Go to each URL.
    """
    # Use Playwright to go to the URL


def count_top_level_menu_elements(html: str) -> int:
    """
    Step 3. Count number of top-level menu elements and save to txt file.
    """


def randomly_select_top_level_menu_element(html: str) -> str:
    """
    Step 4. Randomly select top-level menu element and click on it.
    """


def walk_nested_menu_element(html: str) -> None:
    """
    Step 5. Walk to the bottom of the nested menu element, if applicable. Perform counts of each recursive element along the way.
    """


def randomly_select_final_url(html: str) -> str:
    """
    Step 6. When bottom of subnodes folder is reached, randomly select an element to get to the final URL.
    """


def download_html_to_disk(url: str) -> str:
    """
    Step 7. Download the HTML of the final URL to disk.
    """


def  get_total_size_of_html_files(directory: str) -> int:
    """
    Step 9. Get the total size of the HTML documents in the HTML directory.
    """


async def main():

    logger.info("Begin __main__")

    next_step("Step 1. Get 30 URLs from the CSV.")

    next_step("Step 2. Go to each URL.")

    next_step("Step 3. Count number of top-level menu elements and save to txt file.")

    next_step("Step 4. Randomly select top-level menu element and click on it.")

    next_step("Step 5. Walk to the bottom of the nested menu element, if applicable. Perform counts of each recursive element along the way.")

    next_step("Step 6. When bottom of subnodes folder is reached, randomly select an element to get to the final URL.")
              
    next_step("Step 7. Download the HTML of the final URL to disk.")

    next_step("Step 8. Repeat steps 2-8 for each URL.")

    next_step("Step 9. Get the total size of the HTML documents in the HTML directory.")

    logger.info("End __main__")

    sys.exit(0)


if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = os.path.split(os.path.split(__file__)[0])[1] if base_name != "main.py" else os.path.splitext(base_name)[0] 
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"'{program_name}' program stopped.")

