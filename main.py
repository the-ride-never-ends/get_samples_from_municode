
import asyncio
import os
import sys
from typing import Any, NamedTuple

import pandas as pd
from playwright.async_api import async_playwright


from utils.shared.next_step import next_step
from utils.shared.load_from_csv_via_pandas import load_from_csv_via_pandas
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.get_total_size_of_files_with_specified_type_in_gigabytes import (
    get_total_size_of_files_with_specified_type_in_gigabytes
)
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

import numpy as np


def analyze_token_distributions(documents):
    # Across-document average
    doc_lengths = [len(doc_tokens) for doc in documents]
    across_avg = np.mean(doc_lengths)
    
    # Within-document average
    total_tokens = sum(doc_lengths)
    within_avg = total_tokens / len(documents)
    
    # Additional context
    length_variance = np.var(doc_lengths)
    length_ratio = max(doc_lengths) / min(doc_lengths)
    
    return {
        "across_document_avg": across_avg,
        "within_document_avg": within_avg,
        "length_variance": length_variance,
        "max_min_ratio": length_ratio
    }


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


    for file in html_files:
        logger.info(f"Processing file: {file}")
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
        """,f=True)

    average_per_file = sum(total_tokens) / len(html_files) if html_files else 0

    logger.info(f"""
    Total HTML Files: {len(html_files):,}
    Total tokens: {sum(total_tokens):,}
    Average tokens per file: {average_per_file:,}
    """,f=True)

    return average_per_file

from scipy import stats

def estimate_corpus_tokens(sample_tokens, sample_size, total_population_size):
    # Calculate sample statistics
    mean_tokens = np.mean(sample_tokens)
    std_tokens = np.std(sample_tokens, ddof=1)  # Using n-1 for sample std
    
    # Standard error of the mean
    sem = std_tokens / np.sqrt(sample_size)
    
    # Total estimate
    total_estimate = mean_tokens * total_population_size
    
    # Calculate margin of error (95% confidence)
    t_value = stats.t.ppf(0.975, df=sample_size-1)
    margin_of_error = t_value * sem * total_population_size
    
    # Confidence interval for total
    ci_lower = total_estimate - margin_of_error
    ci_upper = total_estimate + margin_of_error
    
    # Coefficient of variation (to assess reliability)
    cv = (std_tokens / mean_tokens) * 100
    
    logger.info(f"""
    Estimated Total Tokens: {total_estimate:,.0f}
    Confidence Interval: ({ci_lower:,.0f}, {ci_upper:,.0f})
    Coefficient of Variation: {cv:.2f}%
    Relative Margin of Error: {(margin_of_error / total_estimate) * 100:.2f}%
    """, f=True)

    return total_estimate

from development.get_count_of_unique_pages import get_count_of_unique_pages

def calculate_stats_for_urls_per_municode_library_page_csv(csv_ending: str = "_unnested.csv") -> None:

    # Initialize counts and constants
    total_municode_source_urls = 3528
    est_tokens_per_unique_page = 100 #estimate_total_tokens_from_html_files()
    csv_count = 0
    url_count_list = []

    # Get a count of the unique pages for every CSV in the output folder.
    # A unique page is defined as a URL that is at the 2nd to last level of a parent hierarchy.
    for file in os.listdir(OUTPUT_FOLDER):
        if file.endswith(csv_ending):
            path = os.path.join(OUTPUT_FOLDER, file)
            count = get_count_of_unique_pages(path)
            url_count_list.append(count)
            csv_count += 1
        else:
            pass

    # Calculate the stats then print.
    url_count = sum(url_count_list) # N
    url_count_mean = st.mean(url_count_list).__round__() # Mean
    url_count_median = st.median(url_count_list).__round__() # Median
    url_count_mode = st.mode(url_count_list) # Mode
    url_count_standard_deviation = st.stdev(url_count_list).__round__() # Standard Deviation

    est_total_urls = round(url_count_mean * total_municode_source_urls)
    est_total_tokens = round(est_total_urls * est_tokens_per_unique_page)

    logger.info(f"""
    Mean Unique URLs per municode library page CSV: {url_count_mean:,}
    Median Unique URLs per municode library page CSV: {url_count_median:,}
    Mode Unique URLs per municode library page CSV: {url_count_mode:,}
    Standard Deviation of Unique URLs per municode library page CSV: {url_count_standard_deviation:,}
    Estimated Number of Token per Unique Page: {est_tokens_per_unique_page:,}
    ############################
    Total CSV count: {csv_count:,}
    Total URL count: {url_count:,}
    Estimated Total Unique URLs on Municode: {est_total_urls:,}
    Estimated Total Tokens on Municode (assuming {est_tokens_per_unique_page:,} per library page): {est_total_tokens:,}
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
    # TODO This currently does not work. It returns 0.0 GB.
    _ = get_total_size_of_files_with_specified_type_in_gigabytes(OUTPUT_FOLDER)

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

