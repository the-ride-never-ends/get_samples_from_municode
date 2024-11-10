
import asyncio
import os
import sys


import pandas as pd
from playwright.async_api import async_playwright


from utils.shared.next_step import next_step
from utils.shared.load_from_csv_via_pandas import load_from_csv_via_pandas
from utils.shared.get_total_size_of_files_with_specified_type_in_gigabytes import (
    get_total_size_of_files_with_specified_type_in_gigabytes
)

from web_scraper.sites.municode.library.ScrapeMunicodePage import ScrapeMunicodePage

from config.config import OUTPUT_FOLDER, RANDOM_SEED
from logger.logger import Logger
logger = Logger(logger_name=__name__)


async def main():

    logger.info("Begin __main__")

    next_step("Step 1. Get URLs from the CSV.")
    name = ["gnis, place_name, url"]
    header_line = 0
    count_list = []
    urls_df: pd.DataFrame = load_from_csv_via_pandas("input_urls.csv")

    next_step("Step 2. Scrape each URL.")
    async with async_playwright() as pw_instance:
        scraper: ScrapeMunicodePage = await ScrapeMunicodePage.start(domain="https://municode.com/", pw_instance=pw_instance, headless=False)
        for row in urls_df.itertuples():
            logger.info(f"Processing URL: {row.url}")

            next_step("Step 2.1 Go to each URL.")
            await scraper.navigate_to(url=row.url)

            next_step("Step 2.2 Count number of top-level menu elements.")
            count_list = await scraper.count_top_level_menu_elements(count_list)

            next_step("Step 2.3 Scrape Municode's Table of Contents nested menu, save it to CSV, and return a pandas DataFrame.")
            df = await scraper.scrape_municode_toc_menu()

            next_step("Step 2.4 Randomly select a URL from the DataFrame to get to the final URL.")
            url = scraper.randomly_select_final_url(df, seed=RANDOM_SEED)

            next_step("Step 2.5 Download the HTML of the final URL to disk.")
            await scraper.download_html_to_disk(url)

        await scraper.exit()

    next_step("Step 3. Get the total size of the HTML documents in the HTML directory.")
    size = get_total_size_of_files_with_specified_type_in_gigabytes(OUTPUT_FOLDER)

    logger.info(f"Total size of HTML documents: {size} gigabytes.\nEnd __main__")

    sys.exit(0)

if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = os.path.split(os.path.split(__file__)[0])[1] if base_name != "main.py" else os.path.splitext(base_name)[0] 
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"'{program_name}' program stopped.")

