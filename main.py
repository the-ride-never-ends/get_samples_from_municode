
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


from manual.unnest_csv import unnest_csv
from utils.read_csv import read_csv
UNNEST_CSV_ROUTE = False

from pandas_dataframe_row import MuniRow

def unnest_csv_step(df: pd.DataFrame=None, row: NamedTuple=None) -> None|pd.DataFrame:
    if df is None:
        if UNNEST_CSV_ROUTE:
            logger.info("Performing unnesting of specified CSV files in output folder...")
            for file in os.listdir(OUTPUT_FOLDER):
                logger.debug(f"file: {file}")
                base_name = os.path.basename(file)
                if file.endswith("traversal_results.csv"):
                    unnested_csv_path = os.path.join(OUTPUT_FOLDER, base_name.replace(".csv", "_unnested.csv"))
                    try:
                        unnest_csv(os.path.join(OUTPUT_FOLDER, file), unnested_csv_path)
                    except Exception as e:
                        logger.error(f"Error unnesting {file}: {e}")
                        raise e
            logger.debug("Finished unnesting CSV files. Exiting...")
            sys.exit(0)
    else:
        if row is None:
            raise ValueError("row cannot be None if df is not None")
        else:
            logger.info("Performing unnesting of input dataframe...")
            place_name = row.place_name.replace(" ", "_").lower()
            base_name = place_name + "_" + str(row.gnis) + "_menu_traversal_results_unnested.csv"
            unnested_csv_path = os.path.join(OUTPUT_FOLDER, base_name)
            return unnest_csv(df, unnested_csv_path)


async def main():

    logger.info("Begin __main__")

    unnest_csv_step()

    next_step("Step 1. Get URLs from the CSV.")
    name = ["gnis, place_name, url"]
    header_line = 0
    count_list = []
    input_urls_df: pd.DataFrame = pd.read_csv(os.path.join(INPUT_FOLDER, ("input_urls.csv")))
    output_urls_df: pd.DataFrame = pd.read_csv(os.path.join(INPUT_FOLDER, ("output_urls.csv")))

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

            place_name = row.place_name.replace(" ", "_").lower()
            filename = f"{place_name}_{row.gnis}_menu_traversal_results.csv"
            file_path = os.path.join(OUTPUT_FOLDER, filename)
            logger.debug(f"filename: {filename}", off=True)

            if row.gnis in output_urls_df['gnis'].values:
                logger.info(f"Skipping URL: {row.url} because the GNIS {row.gnis} is already in the output_urls.csv file")
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
            df = unnest_csv_step(df, row)

            next_step("Step 2.5 Randomly select a URL from the DataFrame to get to the final URL.")
            url = randomly_select_value_from_pandas_dataframe_column('url', df, seed=RANDOM_SEED)

            next_step("Step 2.6 Download the HTML of the final URL to disk.")
            await scraper.download_html_to_disk(url)

            next_step(f"Step 2.7 Save the DataFrame to CSV in the output folder.")	
            new_row_df = pd.DataFrame(
                {'gnis': [row.gnis], 
                 'place_name': [row.place_name], 
                 'url': [row.url], 
                 'source_place_domain': [row.source_place_domain]}
            )
            new_row_df.to_csv(os.path.join(INPUT_FOLDER, "output_urls.csv"), index=False, mode='a', header=False)

        await scraper.exit()

    next_step("Step 3. Get the total size of the HTML documents in the HTML directory.")
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

