
import asyncio
import os
import sys
from typing import Any, NamedTuple

import pandas as pd
from playwright.async_api import async_playwright


from utils.shared.next_step import next_step
from utils.shared.load_from_csv_via_pandas import load_from_csv_via_pandas
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.randomly_select_value_from_pandas_dataframe_column import (
    randomly_select_value_from_pandas_dataframe_column
)

from web_scraper.sites.municode.library.scrape_municode_library_page import ScrapeMunicodeLibraryPage

from config.config import OUTPUT_FOLDER, RANDOM_SEED, INPUT_FOLDER, INPUT_FILENAME
from logger.logger import Logger
logger = Logger(logger_name=__name__)


from validated.unnest_csv_step import unnest_csv_step
UNNEST_CSV_ROUTE = False

from development.pandas_dataframe_row import MuniRow


def row_is_in_this_dataframe(value: Any, column: str, df: pd.DataFrame) -> bool:
    """
    Check if a value exists in a specific column of a pandas DataFrame.

    Args:
        value (Any): The value to search for.
        column (str): The column name to search in.
        df (pd.DataFrame): The pandas DataFrame to search.

    Returns:
        bool: True if the value is found, False otherwise.
    """
    return value in df[column].values

from validated.append_pandas_row_to_csv import append_pandas_row_to_csv


import statistics as st

from bs4 import BeautifulSoup
import tiktoken as tk


import tqdm


def estimate_total_tokens_from_html_files():
    """
    Example
        chunk_texts = get_stats_from_saved_html()
        for i, text in enumerate(chunk_texts, 1):
            print(f"Chunk {i}: {text[:100]}...")  # Print first 100 characters of each chunk
    """
    # Define constants
    class_ = "chunk-content-wrapper"
    dir_path = os.path.join(OUTPUT_FOLDER, "scrape_municode_library_page")
    encoding = tk.encoding_for_model("gpt-4o")

    # Initialize variables
    average_tokens_per_file = []
    total_tokens = []

    # Get the html files in the directory and how many of them there are.
    html_files = [file for file in os.listdir(dir_path) if file.endswith(".html")]


    for file in tqdm.tqdm(html_files, desc="Processing HTML files", unit="file"):
        #logger.info(f"Processing file: {file}")
        html_chunk_content = []

        # Read the HTML file
        with open(os.path.join(dir_path, file), "r", encoding="utf-8") as f:
            html_content = f.read()

        # Parse it.
        soup = BeautifulSoup(html_content, "html.parser")

        # Find all elements with class 'chunk-content-wrapper'
        chunk_content_wrappers = soup.find_all(class_=class_)
        if chunk_content_wrappers == 0:
            print(f"No text under class '{class_}' found in HTML. Skipping...")
            continue

        total_num_wrappers = len(chunk_content_wrappers)

        # Get the token count for each text.
        # NOTE We don't need the tokens themselves, just how many of them there are.
        html_chunk_content = [
            len(encoding.encode(wrapper.get_text(strip=True))) for wrapper in chunk_content_wrappers
        ]

        # Calculate basic statistics for this HTML file.
        total_tokens_in_file = sum(html_chunk_content)
        if total_num_wrappers > 0:
            average_tokens_per_chunk = total_tokens_in_file / total_num_wrappers # Average
        else:
            average_tokens_per_chunk = 0

        total_tokens.append(total_tokens_in_file)
        average_tokens_per_file.append(average_tokens_per_chunk) 

        logger.info(f"""
        HTML File: {file}
        Total chunks: {total_num_wrappers:,}
        Total tokens: {total_tokens_in_file:,}
        Average tokens per chunk: {average_tokens_per_chunk:,}
        """,f=True,off=True)

    average_per_file = sum(total_tokens) / len(html_files) if html_files else 0

    logger.info(f"""
    Total HTML Files: {len(html_files):,}
    Total tokens: {sum(total_tokens):,}
    Average tokens per file: {average_per_file:,}
    """,f=True)

    return average_per_file






from development.get_count_of_unique_pages import get_count_of_unique_pages
from development.get_stats_of_html_files_in_this_directory import (
    get_stats_of_html_files_in_this_directory
)

def calculate_stats_for_urls_per_municode_library_page_csv(csv_ending: str = "_unnested.csv") -> None:


    # Initialize counts and constants
    TOTAL_MUNICODE_SOURCE_URLS = 3528
    COST_PER_GIGABYTE_IN_DOLLARS = 8.4
    est_tokens_per_unique_page = estimate_total_tokens_from_html_files()
    csv_count = 0
    url_count_list = []

    # Get a count of the unique pages for every CSV in the output folder.
    # A unique page is defined as a URL that is at the 2nd to last level of a parent hierarchy.
    for file in os.listdir(OUTPUT_FOLDER):
        if file.endswith(csv_ending):
            path = os.path.join(OUTPUT_FOLDER, file)
            url_count_list.append(get_count_of_unique_pages(path))
            csv_count += 1

    html_folder = os.path.join(OUTPUT_FOLDER, 'scrape_municode_library_page')
    average_file_size = get_stats_of_html_files_in_this_directory(html_folder)

  


    # Calculate the stats then print.
    url_count = sum(url_count_list) # N
    url_count_mean = st.mean(url_count_list).__round__() # Mean
    url_count_median = st.median(url_count_list).__round__() # Median
    url_count_mode = st.mode(url_count_list) # Mode
    url_count_standard_deviation = st.stdev(url_count_list).__round__() # Standard Deviation

    est_total_urls = round(url_count_mean * TOTAL_MUNICODE_SOURCE_URLS)
    est_total_tokens = round(est_total_urls * est_tokens_per_unique_page)

    total_size_of_municode_in_gigabytes = url_count_mean * average_file_size
    total_cost_to_scrape = total_size_of_municode_in_gigabytes / COST_PER_GIGABYTE_IN_DOLLARS

    logger.info(f"""
    Unique Pages per municode library page CSV:
    - Mean: {url_count_mean:,}
    - Median: {url_count_median:,}
    - Mode: {url_count_mode:,}
    - Standard Deviation: {url_count_standard_deviation:,}
    - Estimated Number of Token per Unique Page: {est_tokens_per_unique_page:,}
    - Mean Filesize: {average_file_size:.2f} gigabytes
    ############################
    - Total CSV count: {csv_count:,}
    - Total Unique Page count: {url_count:,}
    - Estimated Size of Unique Pages on Municode: {total_size_of_municode_in_gigabytes:.2f} gigabytes
    - Estimated Total Unique Pages on Municode: {est_total_urls:,}
    - Estimated Total Tokens on Municode (assuming {est_tokens_per_unique_page:,} per library page): {est_total_tokens:,}
    - Total Cost to Scrape at ${COST_PER_GIGABYTE_IN_DOLLARS:.2f} USD per gigabyte: ${total_cost_to_scrape:,.2f} USD
    """,f=True, t=60)
    print("Exiting...")
    return



# place_name = row.place_name.replace(" ", "_").lower()
# filename = f"{place_name}_{row.gnis}_menu_traversal_results.csv"
# file_path = os.path.join(OUTPUT_FOLDER, filename)
# logger.debug(f"filename: {filename}", off=True)


MANUAL_USE = True

async def main():

    logger.info("Begin __main__")

    if MANUAL_USE:
        logger.info("MANUAL_USE ACTIVE: calculate_stats_for_urls_per_municode_library_page_csv")
        calculate_stats_for_urls_per_municode_library_page_csv()
        sys.exit(0)

    # unnest_csv_step(logger=logger, UNNEST_CSV_ROUTE=True)

    next_step("Step 1. Get URLs from the CSV.")
    name = ["gnis, place_name, url"]
    header_line = 0
    count_list = []
    input_urls_df: pd.DataFrame = pd.read_csv(os.path.join(INPUT_FOLDER, ("input_urls.csv")))
    output_urls_df: pd.DataFrame = pd.read_csv(os.path.join(INPUT_FOLDER, ("output_urls.csv")))
    malformed_urls_df: pd.DataFrame = pd.read_csv(os.path.join(INPUT_FOLDER, ("malformed_urls.csv")))

    next_step("Step 2. Scrape each URL.")
    async with async_playwright() as pw_instance:
        scraper: ScrapeMunicodeLibraryPage = await ScrapeMunicodeLibraryPage.start(
            domain="https://municode.com/", 
            pw_instance=pw_instance, 
            headless=False
        )

        for row in input_urls_df.itertuples(): 
            # -> NamedTuple(Index=0, gnis=12345, place_name='City of Example', url='https://example.com'):
            logger.info(f"Processing URL: {row.url}")

            if row_is_in_this_dataframe(row.gnis, 'gnis', output_urls_df):
                logger.info(f"Skipping URL: {row.url} because the GNIS {row.gnis} is already in the output_urls.csv file")
                continue

            if "%2C" in row.url or "," in row.url or row_is_in_this_dataframe(row.gnis, 'gnis', malformed_urls_df):
                logger.info(f"Skipping URL: {row.url} because it contains '%2c' or ','. This will produce a 404 if loaded.")
                append_pandas_row_to_csv(row, "malformed_urls.csv")
                continue

            next_step("Step 2.1 Go to each URL.")
            await scraper.navigate_to(url=row.url)

            next_step("Step 2.2 Count number of top-level menu elements.")
            count_list = await scraper.count_top_level_menu_elements(count_list)

            next_step("Step 2.3 Scrape Municode's Table of Contents nested menu, save it to CSV, and return a pandas DataFrame.")
            df = await scraper.scrape_municode_toc_menu(row)
            if df is None:
                continue

            next_step("Step 2.4 Flatten the nested dataframe.")
            df = unnest_csv_step(df, row, logger=logger, UNNEST_CSV_ROUTE=UNNEST_CSV_ROUTE)

            next_step("Step 2.5 Randomly select a URL from the DataFrame to get to the final URL.")
            url = randomly_select_value_from_pandas_dataframe_column('url', df, seed=RANDOM_SEED)

            next_step("Step 2.6 Download the HTML of the final URL to disk.")
            await scraper.download_html_to_disk(url)

            next_step(f"Step 2.7 Append the rows DataFrame to output_urls.csv in the output folder.")
            append_pandas_row_to_csv(row, "output_urls.csv")

        await scraper.exit()

    next_step("Step 3. Get the total size of the HTML documents in the HTML directory.")


    logger.info(f"End __main__")

    sys.exit(0)

if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = os.path.split(os.path.split(__file__)[0])[1] if base_name != "main.py" else os.path.splitext(base_name)[0] 
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"'{program_name}' program stopped.")

