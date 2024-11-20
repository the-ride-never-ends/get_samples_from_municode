import asyncio
import csv
import os


import pandas as pd
from tqdm import tqdm

from development.scrape_for_doc_content.insert_urls_df_into_urls_table import insert_urls_df_into_urls_table
from development.scrape_for_doc_content.split_city_name_and_gnis_from_filename_suffix import (
    split_city_name_and_gnis_from_filename_suffix
)
from development.scrape_for_doc_content.get_gnis_df_and_url_hash_df_from_mysql_database import (
    get_gnis_df_and_url_hash_df_from_mysql_database
)
from development.scrape_for_doc_content.format_csv_files_with_suffix_for_import_into_urls_table_in_sql_database import (
    format_csv_files_with_suffix_for_import_into_urls_table_in_sql_database
)
from development.scrape_for_doc_content.load_municode_urls_from_mysql_database import (
    load_municode_urls_from_mysql_database
)
from development.scrape_for_doc_content.load_municode_urls_from_csv_files import load_municode_urls_from_csv_files
from development.scrape_for_doc_content.load_unnested_urls_from_csv import load_unnested_urls_from_csv


from utils.shared.next_step import next_step
from database.utils.database.get_column_names import get_column_names
from database.utils.database.get_num_placeholders import get_num_placeholders
from database.utils.database.get_columns_to_update import get_columns_to_update

from database.MySqlDatabase import MySqlDatabase
from config.config import OUTPUT_FOLDER
from logger.logger import Logger
logger = Logger(logger_name=__name__)


# for file in tqdm.tqdm(html_files, desc="Processing HTML files", unit="file"):

#     with open(os.path.join(dir_path, file), "r", encoding="utf-8") as f:
#         html_content = f.read()

#     soup = BeautifulSoup(html_content, "html.parser")

#     # Find all elements with class 'chunk-content-wrapper'
#     chunk_content_wrappers = soup.find_all(class_=class_)
#     if chunk_content_wrappers == 0:
#         # print(f"No text under class '{class_}' found in HTML. Skipping...")
#         continue

#     # Get the token count for each text.
#     # NOTE We don't need the tokens themselves, just how many of them there are.
#     html_chunk_content = [
#         len(encoding.encode(wrapper.get_text(strip=True))) for wrapper in chunk_content_wrappers
#     ]


from web_scraper.playwright.async_.async_playwright_scraper import AsyncPlaywrightScraper


class ScrapeForDocContent(AsyncPlaywrightScraper):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def scrape_for_doc_content(self, df: pd.DataFrame) -> None:

        len_df = len(df)
    
        for idx, row in enumerate(df.itertuples(), start=1):
            logger.info(f"{idx}/{len_df} - {row.url}")
            url = row.url
            gnis = row.gnis
            self.navigate_to(url, idx=idx)








async def scrape_for_doc_content() -> None:
    """
    Algorithm: Scrape and Process Document Content

    1. Load unnested URLs from CSV files

    2. For each CSV file:
        a. If CSV data is available:
            - Skip downloading from database
        b. Else:
            - Download URLs from database
        c. Create url_list from CSV or database data
        d. Order url_list by node depth

    3. For each url in url_list:
        a. Get the page title
        - If title contains "Mini Toc":
            - Remove URL from list
            - Continue to next URL
        b. Get the page HTML
    
        c. For each text chunk in HTML:
            - Extract text chunk and its ID
        - If text chunk ID is in url_list:
            - Remove it
            - Perform basic cleaning of text using HTML parse library
            - Save text chunk to database with metadata
    
    d. After processing all chunks:
        - Remove the input URL from url_list

    4. Move to next URL in url_list
    """


    next_step("Step 1: Load unnested URLs from CSV files as pandas dataframes")
    suffix = "_menu_traversal_results_unnested"
    output_suffix = "sql_ready_urls"
    folder_path, unnested_urls_df_tuple_list = load_unnested_urls_from_csv(suffix, output_suffix)
    logger.info("unnested_urls_df_tuple_list created successfully.")
    logger.debug(f"unnested_urls_df_tuple_list[0]: {unnested_urls_df_tuple_list[0]}")


    next_step("Step 2: Format dataframes with url_hash, query_hash, gnis, url, municode_url_depth", stop=True)
    output_folder_list = [
        format_csv_files_with_suffix_for_import_into_urls_table_in_sql_database(tup[0], tup[1], suffix) 
        for tup in unnested_urls_df_tuple_list
    ]


    next_step("Step 3: Upload the sql ready URLs csv to the MySQL database", stop=True)
    async with MySqlDatabase(database="socialtoolkit") as db:
        for output_folder in tqdm(output_folder_list, desc="Uploading URLs to MySQL database"):
            await insert_urls_df_into_urls_table(output_folder, db)


        next_step("""
        Step 4: Create urls_list based on whether a GNIS has a source_municode.
        If we got a csv of the GNIS, skip downloading them from the database.
        Else download them from the database.
        """, stop=True)

        urls_df_list = []
        gnis_df, url_hash_df = await get_gnis_df_and_url_hash_df_from_mysql_database(db)

        urls_df_list = load_municode_urls_from_csv_files(
                            folder_path, 
                            output_suffix, 
                            url_hash_df=url_hash_df, 
                            gnis_df=gnis_df, 
                            urls_df_list=urls_df_list)
        
        urls_df_list = await load_municode_urls_from_mysql_database(db, gnis_df, urls_df_list)


        next_step("""Step 5: DRAW THE REST OF THE FUCKING OWL""", stop=True)

        for urls_df in urls_df_list:
            pass







if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = os.path.split(os.path.split(__file__)[0])[1] if base_name != "main.py" else os.path.splitext(base_name)[0] 
    try:
        asyncio.run(scrape_for_doc_content())
    except KeyboardInterrupt:
        print(f"'{program_name}' program stopped.")




