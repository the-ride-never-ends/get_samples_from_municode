
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


from development.row_is_in_this_dataframe import row_is_in_this_dataframe

from validated.append_pandas_row_to_csv import append_pandas_row_to_csv


from development.estimate_average_tokens_per_page_from_html_files import (
    estimate_average_tokens_per_page_from_html_files
)
from development.get_count_of_unique_pages import get_count_of_unique_pages
from development.get_stats_of_html_files_in_this_directory import (
    get_stats_of_html_files_in_this_directory
)
from development.calculate_stats_for_urls_per_municode_library_page_csv import (
    calculate_stats_for_urls_per_municode_library_page_csv
)

from database.MySqlDatabase import MySqlDatabase

MANUAL_USE = True

async def main():

    logger.info("Begin __main__")

    if MANUAL_USE:
        logger.info("MANUAL_USE ACTIVE: calculate_stats_for_urls_per_municode_library_page_csv")
        #calculate_stats_for_urls_per_municode_library_page_csv()
        with MySqlDatabase(database="socialtoolkit") as db:
            for csv in os.listdir(os.path.join(OUTPUT_FOLDER, "sql_ready_urls")):
                if csv.endswith("_sql_ready_urls.csv"):
                    data = pd.read_csv(csv)
                    logger.debug(f"data: {data}", t=30)
                await db.async_execute_sql_command

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

    # next_step("Step 3. Get the total size of the HTML documents in the HTML directory.")


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

