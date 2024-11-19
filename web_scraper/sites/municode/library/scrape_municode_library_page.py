import asyncio
import random
from typing import NamedTuple


import pandas as pd
from playwright.async_api import (
    async_playwright,
    expect,
    Locator,
    Error as AsyncPlaywrightError,
    TimeoutError as AsyncPlaywrightTimeoutError
)


from web_scraper.playwright.async_.async_playwright_scraper import AsyncPlaywrightScraper
from .table_of_contents.walk_municode_toc import WalkMunicodeToc

from utils.shared.make_sha256_hash import make_sha256_hash
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.make_path_from_function_name import make_path_from_function_name
from utils.shared.save_to_csv import save_to_csv
from utils.shared.decorators.try_except import async_try_except

from config.config import *

from logger.logger import Logger
logger = Logger(logger_name=__name__)


class ScrapeMunicodeLibraryPage(AsyncPlaywrightScraper):
    """
    Scrape a Municode library page.
    This class is gets 3 important elements from the page:
    1. The table of contents
    2. The versions of codes
    3. The list of documents
    """

    NODE_ID_SELECTOR  = 'a[href*="?nodeId="]' # NOTE This selector works.

    def __init__(self,
                domain: str,
                pw_instance,
                *args,
                user_agent: str = "*",
                **kwargs):
        super().__init__(domain, pw_instance, *args, user_agent=user_agent, **kwargs)

        output_folder = os.path.join(OUTPUT_FOLDER, "scrape_municode_library_page")
        if not os.path.exists(output_folder):
            print(f"Creating output folder: {output_folder}")
            os.mkdir(output_folder)

        self.output_folder: str = output_folder
        self.place_name: str = sanitize_filename(domain)


    async def screen_shot_frontpage(self, page):
        """
        Take a screenshot of the frontpage
        """
        file_path = os.path.join(self.output_folder, f"{self.place_name}_frontpage.png")
        await self.navigate_to(page, self.domain)
        await self.page.screenshot(path=file_path)
        return


    async def count_top_level_menu_elements(self, count_list: list[int]) -> None: # -> list[int]
        """
        Get the top-level node elements from the Table of Contents.
        """
        logger.debug(f"Selecting top-level node elements with root_selector {self.NODE_ID_SELECTOR}...")
        try:
            # Select all the root anchors.
            root_anchors = await self.page.query_selector_all(self.NODE_ID_SELECTOR)
            logger.info(f"Found {len(root_anchors)} top-level node elements with root_selector {self.NODE_ID_SELECTOR}")

            # Put them in the input_list
            count_list.append(len(root_anchors))
        except (AsyncPlaywrightError, AsyncPlaywrightTimeoutError) as e:
            logger.error(f"Error selecting top-level node elements with root_selector {self.NODE_ID_SELECTOR}: {e}")

        return count_list


    async def download_html_to_disk(self, url: str, idx: int=None) -> None:
        """
        Navigate to the given URL and download the HTML content to disk.
        """
        try:
            await self.navigate_to(url, idx=idx)

            # Get the HTML content
            html_content = await self.page.content()
            
            # Create a filename based on the URL
            filename = sanitize_filename(url) + '.html'
            filepath = os.path.join(self.output_folder, filename)
            
            # Write the HTML content to disk
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)

            logger.info(f"Successfully downloaded HTML from {url} to {filepath}.")
            await self.close_current_page_and_context()
        
        except (AsyncPlaywrightError, AsyncPlaywrightTimeoutError) as e:
            logger.error(f"Playwright error while downloading HTML from {url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while downloading HTML from {url}: {e}")
        return

    async def screenshot_if_no_menu_elements(self, row: str) -> None:
        logger.info(f"Skipping URL: {row.url} because it errored. Taking screenshot and continuing to next row...")
        await self.take_screenshot(filename=f"{sanitize_filename(row.url)}_{row.gnis}_no_menu_elements.png")
        await self.close_current_page_and_context()
        return None

    async def scrape_municode_toc_menu(self, row: NamedTuple) -> pd.DataFrame|None:
        """
        Scrape a Table of Contents menu from Municode.
        
        Args:
            seed (int): Random seed for reproducibility.
            row (NamedTuple): A row from the dataframe containing the URL and place name.
            
        Returns:
            tuple: (selected_element, element_text, element_href)
        """

        try:
            # Wait for the top-level menu elements to be visible
            await self.page.wait_for_selector(self.NODE_ID_SELECTOR , state='visible', timeout=10000)

            # Create walk instance
            walk = WalkMunicodeToc(self.page, self.place_name, self.output_folder)

            # Walk the nested menu and save the results.
            df: pd.DataFrame = await walk.nested_menu(self.NODE_ID_SELECTOR, row)
            if df is None or len(df) == 0:
                logger.warning(f"Selector '{self.NODE_ID_SELECTOR}' was found for {row.url} but could not find menu elements.")
                await self.screenshot_if_no_menu_elements(row)
                return None
            else:
                logger.info("Walk of Municode ToC menu finished.")
                logger.debug(f"df\n{df.head()}",f=True)

            # Save the HTML from the webpage once the nodes have been expanded.
            logger.info("Nodes expanded successfully.\nGetting HTML...")
            html = await self.page.inner_html('body')
            html_filepath = make_path_from_function_name(f"{sanitize_filename(self.page.url)}.html")
            with open(html_filepath, 'w', encoding='utf-8') as file:
                file.write(html)

            logger.info(f"{os.path.basename(html_filepath)} successfully saved to output folder.")
            await self.close_current_page_and_context()
            return df

        except (AsyncPlaywrightTimeoutError, AsyncPlaywrightError) as e:
            logger.error(f"Playwright Error in scrape_municode_toc_menu: {e}")
            await self.screenshot_if_no_menu_elements(row)
            return None
        except Exception as e:
            logger.error(f"Error in scrape_municode_toc_menu: {e}")
            raise


class GetMunicodeSidebarElements(AsyncPlaywrightScraper):
    """
    Get the sidebar elements from a Municode URL page.
    """

    def __init__(self,
                domain: str,
                pw_instance,
                *args,
                user_agent: str="*",
                **kwargs):
        super().__init__(domain, pw_instance, *args, user_agent=user_agent, **kwargs)

        self.output_folder:str = None
        self.place_name:str = None

    def test(self):
        self.page.set_content()

    async def _get_past_front_page(self) -> bool:
        """
        Figure out what kind of front page we're on. If it's a regular page, return it.
        """
        # See whether or not the ToC button is on the page.
        toc_button_locator: Locator = await self.page.get_by_text("Browse table of contents")
        expect(toc_button_locator).to_be_visible()


    async def _choose_browse_when_given_choice(self):
        """
        Choose to browse the table of contents if given the choice between that and Municode's documents page.
        """
        pass

    async def is_regular_municode_page(self) -> bool:
        # Define the selector for the button.
            # As the all 'regular' pages on Municode have a sidebar, 
            # we can use the presence of the sidebar to determine if we're on a 'regular' page.
        button_selector = '#codebankToggle button[data-original-title="CodeBank"]'

        # Wait 5 seconds for the button to be visible
        try:
            await self.page.wait_for_selector(button_selector, state='visible', timeout=5000)
            logger.info("Codebank button visible.")
            return True
        except:
            logger.info("CodeBank button not visible. ")
            self.take_screenshot(
                self.page.url,
                prefix="is_regular_municode_page",
                full_page=True, 
            )
            return False


    async def get_code_version_button_texts(self, max_retries: int=3, retry_delay: int=1) -> list[str]:
        counter = 0
        for attempt in range(max_retries):
            try:

                logger.debug(f"Attempt {attempt + 1} of {max_retries} to get button texts")
                
                # Wait for the container first
                await self.page.wait_for_selector("#codebank", timeout=5000)
                
                # Wait a brief moment for Angular rendering
                await asyncio.sleep(0.5)
                
                # Try different selectors
                buttons = await self.page.locator("#codebank button").all()
                if not buttons:
                    buttons = await self.page.locator(".timeline-entry button").all()
                if not buttons:
                    buttons = await self.page.locator(".card-body button").all()
                    
                if buttons:
                    logger.debug(f"Found {len(buttons)} buttons on attempt {attempt + 1}")
                    
                    versions = []
                    for button in buttons:
                        try:
                            # Wait for each button to be stable
                            await button.wait_for(state="attached", timeout=1000)
                            text = await button.text_content()
                            if text and text.strip():
                                versions.append(text.strip())
                        except Exception as e:
                            logger.warning(f"Failed to get text from button: {e}")
                            continue
                    
                    if versions:
                        logger.info(f"Successfully got {len(versions)} version texts")
                        return versions
                        
                logger.warning(f"No valid versions found on attempt {attempt + 1}")
                await asyncio.sleep(retry_delay)

            except TimeoutError as e:
                counter += 1
                logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
                await asyncio.sleep(retry_delay)
            except Exception as e:
                counter += 1
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                await asyncio.sleep(retry_delay)
        
        # If we get here, all retries failed
        logger.exception(f"Failed to get button texts after {max_retries} attempts. Returning...")
        return


    #@async_try_except(exception=[AsyncPlaywrightTimeoutError, AsyncPlaywrightError])
    async def scrape_code_version_popup_menu(self, place_id: str) -> None:
        """
        Scrape a code version pop-up menu
        Returns
            CSV file
        """

        # Define variables
        js_kwargs = {}
        js_kwargs['codebank_label'] = codebank_label = 'CodeBank' # 'span.text-sm text-muted'
        # NOTE Brute force time baby!
        codebank_button = '#codebankToggle button[data-original-title="CodeBank"]'

        logger.info("Waiting for the codebank button to be visible...")
        await self.page.wait_for_selector(codebank_button, state='visible')

        logger.info("Codebank button is visible. Getting current version from it...")
        current_version = await self.page.locator(codebank_button).text_content()
        logger.debug(f"current_version: {current_version}")

        # Hover over and click the codebank to open the popup menu
        logger.info(f"Got current code version '{current_version}'. Hovering over and clicking Codebank button...")
        await self.move_mouse_cursor_to_hover_over(codebank_button)
        await self.page.click(codebank_button)
        
        # Wait for the popup menu to appear
        logger.info("Codebank button was hovered over and clicked successfully. Waiting for popup menu...")
        popup_selector = 'List of previous versions of code'

        # 'aria-label.List of previous versions of code' 
        # NOTE Since the aria-label is hidden, you need to use state='hidden' in order to get it.
        await self.page.wait_for_selector(popup_selector, state='hidden')

        # Go into the CodeBank list and get the button texts.
        # These should be all the past version dates.
        logger.info("Popup menu is visible. Getting previous code versions...")

        # NOTE: This is a CSS selector!
        # 1. First, wait for the timeline to be actually present and visible
        try: 
            await self.page.wait_for_selector("ul.timeline", timeout=5000)
            logger.debug("Timeline UL found")
        except Exception as e:
            logger.error(f"Timeline UL not found: {e}")
            raise

        # 2. Then, get all the buttons inside the timeline
        versions = await self.get_code_version_button_texts()

        if versions:
            # Save the versions to a CSV
            df = pd.DataFrame(versions, columns=['version'])
            df.to_csv(os.path.join(self.output_dir, f'all_code_versions_{place_id}.csv'), index=False, quoting=1)

        return 


    @async_try_except(exception=[AsyncPlaywrightError])
    async def get_municode_sidebar_elements(self, 
                                      i: int,
                                      row: NamedTuple,
                                      len_df: int,
                                      ) -> dict:
        """
        Extract the code versions and table of contents from a city's Municode page.
        NOTE This function orchestrates all the methods of this class, similar to main.py

        Example Input:
            row
            Pandas(Index=0, 
                    url=https://library.municode.com/az/cottonwood, 
                    gnis: 12345, 
                    place_name: Town of Cottonwood,
                    url_hash=ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5...)

        Example Output:
            output_dict = {
                'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
                'input_url': "https://library.municode.com/az/cottonwood",
                'gnis': 123456789,
                'current_code_version': 'July 26th, 2024',
                'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
                'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
            }
        """
        logger.info(f"Processing URL {i} of {len_df}...")

        # Check to make sure the URL is a municode one, then initialize the dictionary.
        assert "municode" in row.url, f"URL '{row.url}' is not for municode."
        input_url = row.url
        self.place_name = place_name = row.place_name.lower().replace(" ", "_")
        place_id = f"{place_name}_{row.gnis}"

        # Skip the webpage if we already got it.
        # output_dict = self._skip_if_we_have_url_already(input_url)
        # if output_dict:
        #     return output_dict

        output_dict = {
            'url_hash': row.url_hash, 
            'input_url': input_url, 
            'gnis': row.gnis
        }

        # Go to the webpage
        await self.navigate_to(input_url, idx=i)
        logger.info("Navigated to input_url")

        # Screenshot the initial page.
        await self.take_screenshot(
            place_id, 
            prefix="navigate_to",
            full_page=True, 
            open_image_after_save=True
        )

        await self.save_page_html_content_to_output_dir(f"{place_id}_opening_webpage.html")

        # See what version of the page we're on.
        regular_page = await self.is_regular_municode_page()

        # Get the current code version and all code versions if we're on a regular page.
        if regular_page: # We assume that all regular Municode pages have a version number sidebar element.
            prefix = "scrape_code_version_popup_menu"
            await self.scrape_code_version_popup_menu(place_id)
            # Close the version menu
            await self.click_on_version_sidebar_closer()
        else:
            logger.info(f"{place_name}, gnis {row.gnis} does not have a regular municode page. Skipping...")
            prefix = "is_not_regular_municode_page"

        # Screenshot the page after running scrape_code_version_popup_menu.
        await self.take_screenshot(
            input_url,
            prefix=prefix,
            full_page=True, 
            open_image_after_save=True
        )

        # Get the html of the opening page and write it to a file.
        await self.save_page_html_content_to_output_dir(f"{place_id}_{prefix}.html")

        return

    #@async_try_except(exception=[AsyncPlaywrightError, AsyncPlaywrightTimeoutError],raise_exception=True)
    async def click_on_version_sidebar_closer(self):
        """
        Click on the 'X' button for the version menu

        #toc > div.zone-body.toc-zone-body > div.toc-wrapper > div
        #toc > div.zone-body.toc-zone-body > div.toc-wrapper > div > button
        """
        logger.debug("Waiting for the codebank 'X' button to be visible...")
        # codebank_button = '#toc button[class="btn btn-icon-toggle btn-default pull-right"]'
        codebank_close_button = "i.md.md-close"
        prefix = "click_on_version_sidebar_closer"

        await self.take_screenshot(
            self.place_name,
            prefix=f"{prefix}_before",
            full_page=True,
        )
        await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}_before.html")

        logger.debug(f"Clicking via JS with selector '{codebank_close_button}'...")
        # args = {"codebank_close_button": codebank_close_button}
        js = """
            let button = document.querySelector("i.md.md-close");
            if (button) {
                button.click();
            }
        """
        await self.page.evaluate(js)
        logger.debug(f"JS button clicking code for selector '{codebank_close_button}' evaluated.")
        await self.take_screenshot(
            self.place_name,
            prefix=f"{prefix}_after",
            full_page=True,
        )

        await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}_after.html")

        return 


async def get_sidebar_urls_from_municode_with_playwright(sources_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get href and text of sidebar elements in a Municode city code URL.
    """

    # Initialize webdriver.
    logger.info("Initializing webdriver...")

    # Define options for the webdriver.
    pw_options = {
        "headless": False
    }
    domain = "https://municode.com/"

    # Get the sidebar URLs and text for each Municode URL
    async with async_playwright() as pw_instance:
        logger.info("Playwright instance initialized successfully.")

        # We use a factory method to instantiate the class to avoid context manager fuckery.
        # TODO MAKE START CODE NOT CURSED.
        municode: GetMunicodeSidebarElements = await GetMunicodeSidebarElements(
                                                        domain, pw_instance, 
                                                        user_agent='*', **pw_options
                                                        ).start(
                                                            domain, pw_instance, 
                                                            user_agent='*', **pw_options
                                                        )
        assert municode.browser is not None, "Browser is None."
        logger.info("GetMunicodeSidebarElements initialized successfully")

        logger.info(f"Starting get_municode_sidebar_elements loop. Processing {len(sources_df)} URLs...")

        # Go through each URL.
        # NOTE This will take forever, but we can't afford to piss off Municode. 
        # Just 385 randomly chosen ones should be enough for a statistically significant sample size.
        # We also only need to do this once.
        list_of_lists_of_dicts: list[dict] = [ 
            await municode.get_municode_sidebar_elements(
                i, row, len(sources_df) # NOTE Adding the 'if row else None' is like adding 'continue' to a regular for-loop.
                ) if row else None for i, row in enumerate(sources_df.itertuples(), start=1)
        ]

        await municode.exit()

    logger.info("get_municode_sidebar_elements loop complete. Flattening...")
    # Flatten the list of lists of dictionaries into just a list of dictionaries.
    output_list = [item for sublist in list_of_lists_of_dicts for item in sublist]
    
    logger.info("get_sidebar_urls_from_municode_with_selenium function complete. Making DataFrames and saving...")
    save_code_versions_to_csv(output_list) # We save first to prevent pandas fuck-upery.
    urls_df = make_urls_df(output_list)

    return urls_df


def make_urls_df(output_list: list[dict]) -> pd.DataFrame:
    """
    Make urls_df

    Example Input:
    >>> output_list = [{
    >>>     'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
    >>>     'input_url': "https://library.municode.com/az/cottonwood",
    >>>     'gnis': 123456789,
    >>>     'current_code_version': 'July 26th, 2024',
    >>>     'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
    >>>     'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
    >>> },...]

    Example Output:
        >>> urls_df.head()
        >>> url_hash    query_hash              gnis    url
        >>> 3beb75cb    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH82TA_ARTIILERETA
        >>> 4648a64b    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH26BU_ARTIINGE_S26-2IMUNTABU
        >>> 58cd5049    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH98ZO_ARTIVSURE_DIV2OREPALORE
        >>> 76205dbb    not_found_from_query    156909  https://ecode360.com/WE1870/document/224899757.pdf
        >>> 30935d36    not_found_from_query    156909  https://ecode360.com/NE0395/document/647960636.pdf
        >>> 792b4192    not_found_from_query    254139  https://ecode360.com/LO1625/document/430360980.pdf
        >>> 792b4192    not_found_from_query    254139  https://ecode360.com/LO1625/document/430360980.pdf
    """
    # Turn the list of dicts into a DataFrame.
    urls_df = pd.DataFrame.from_records(output_list)

    # Make url hashes for each url
    urls_df['url_hash'] = urls_df.apply(lambda row: make_sha256_hash(row['gnis'], row['url']))

    # Rename toc urls to match the format of the table 'urls' in the MySQL database.
    urls_df.rename(columns={"table_of_contents_urls": "url"})

    # Add the dummy query_hash column.
    urls_df['query_hash'] = "not_found_from_query"

    # Drop the code version columns.
    urls_df.drop(['current_code_version','all_code_versions'], axis=1, inplace=True)

    return urls_df



def save_code_versions_to_csv(output_list: list[dict]) -> None:
    """
    Example Input:
    >>> output_list = [{
    >>>     'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
    >>>     'input_url': "https://library.municode.com/az/cottonwood",
    >>>     'gnis': 123456789,
    >>>     'current_code_version': 'July 26th, 2024',
    >>>     'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
    >>>     'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
    >>> },...]
    """

    # Turn the list of dicts into a DataFrame.
    code_versions_df = pd.DataFrame.from_records(output_list)

    # Drop the urls columns.
    code_versions_df.drop(['url_hash','table_of_contents_urls'], axis=1, inplace=True)

    # Save the DataFrame to the output folder.
    output_file = make_path_from_function_name(
                    sanitize_filename(output_list[0]['input_url'])
                )
    save_to_csv(code_versions_df, output_file)

    return

