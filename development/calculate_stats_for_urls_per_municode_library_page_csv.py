import os
import statistics as st


from .estimate_average_tokens_per_page_from_html_files import estimate_average_tokens_per_page_from_html_files
from .get_count_of_unique_pages import get_count_of_unique_pages
from .get_stats_of_html_files_in_this_directory import get_stats_of_html_files_in_this_directory

from config.config import OUTPUT_FOLDER
from logger.logger import Logger
logger = Logger(logger_name=__name__)


def calculate_stats_for_urls_per_municode_library_page_csv(csv_ending: str = "_unnested.csv") -> None:

    # Initialize counts and constants
    TOTAL_MUNICODE_SOURCE_URLS = 3528
    MUNICODE_ROBOTS_TXT_CRAWL_DELAY = 15
    COST_PER_GIGABYTE_IN_DOLLARS = 8.4
    class_ = "chunk-content-wrapper" # This is the HTML class in Municode library pages that contains the text we want to scrape.
    # NOTE 2024-11-18 01:59:44,470
    # *************************
    #     Total HTML Files: 77
    #     Total tokens: 781,217
    #     Average tokens within
    #     Average tokens per file: 10,145.675324675325
    est_tokens_per_unique_page = 10146 or estimate_average_tokens_per_page_from_html_files(class_=class_)
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

    est_total_unique_urls = round(url_count_mean * TOTAL_MUNICODE_SOURCE_URLS)
    est_total_tokens = round(est_total_unique_urls * est_tokens_per_unique_page)

    total_size_of_municode_in_gigabytes = est_total_unique_urls * average_file_size
    total_cost_to_scrape = total_size_of_municode_in_gigabytes * COST_PER_GIGABYTE_IN_DOLLARS

    time_in_days = MUNICODE_ROBOTS_TXT_CRAWL_DELAY * est_total_unique_urls / 60 / 60 / 24

    logger.info(f"""
    Unique Pages per municode library page CSV:
    - Mean: {url_count_mean:,}
    - Median: {url_count_median:,}
    - Mode: {url_count_mode:,}
    - Standard Deviation: {url_count_standard_deviation:,}
    - Estimated Number of Token per Unique Page: {est_tokens_per_unique_page:,.2f} tokens
    - Mean Filesize: {average_file_size:.2f} gigabytes
    ############################
    - Total CSV count: {csv_count:,}
    - Total Unique Page count: {url_count:,}
    - Estimated Size of Unique Pages on Municode: {total_size_of_municode_in_gigabytes:.2f} gigabytes
    - Estimated Total Unique Pages on Municode: {est_total_unique_urls:,}
    - Estimated Total Tokens on Municode (assuming {est_tokens_per_unique_page:,.2f} per library page): {est_total_tokens:,}
    - Total Cost to Scrape with Proxies at ${COST_PER_GIGABYTE_IN_DOLLARS:.2f} USD per gigabyte: ${total_cost_to_scrape:,.2f} USD
    - Total Time to Scrape Concurrently without Proxies (15 second delay per robots.txt): {time_in_days:.2f} days
    """,f=True)
    print("Exiting...")
    return