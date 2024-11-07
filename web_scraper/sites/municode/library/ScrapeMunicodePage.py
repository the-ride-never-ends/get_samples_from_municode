import asyncio
from collections import deque
import random
import re
import time
from typing import NamedTuple

import pandas as pd

from playwright.async_api import (
    async_playwright,
    ElementHandle,
    expect,
    Locator,
    Error as AsyncPlaywrightError,
    TimeoutError as AsyncPlaywrightTimeoutError
)


from web_scraper.playwright.async_.async_playwright_scraper import AsyncPlaywrightScrapper


from utils.shared.make_sha256_hash import make_sha256_hash
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.decorators.adjust_wait_time_for_execution import adjust_wait_time_for_execution
# from utils.shared.load_from_csv import load_from_csv
from utils.shared.save_to_csv import save_to_csv
from utils.shared.decorators.try_except import try_except, async_try_except

from config.config import *

from logger.logger import Logger
logger = Logger(logger_name=__name__)


output_folder = os.path.join(OUTPUT_FOLDER, "get_sidebar_urls_from_municode")
if not os.path.exists(output_folder):
    print(f"Creating output folder: {output_folder}")
    os.mkdir(output_folder)



class ScrapeMunicodePage(AsyncPlaywrightScrapper):
    """
    Scrape a Municode library page
    """
    def __init__(self,
                domain: str,
                pw_instance,
                *args,
                user_agent: str="*",
                **kwargs):
        super().__init__(domain, pw_instance, *args, user_agent=user_agent, **kwargs)
        self.xpath_dict = {
            "current_version": '//*[@id="codebankToggle"]/button/text()',
            "version_button": '//*[@id="codebankToggle"]/button/',
            "version_text_paths": '//mcc-codebank//ul/li//button/text()',
            'toc': "//input[starts-with(@id, 'genToc_')]" # NOTE toc = Table of Contents
        }
        self.queue = deque()
        self.output_folder:str = output_folder
        self.place_name:str = None

    async def screen_shot_frontpage(self, page):
        """
        Take a screenshot of the frontpage
        """

        await self.navigate_to(page, self.domain)
        await self.page.screenshot(path=os.path.join(self.output_folder, f"{self.place_name}_frontpage.png"))

    async def count_top_level_menu_elements(self):
        """
        Get the top-level node elements from the Table of Contents.
        """

    # async def randomly_select_top_level_menu_element(self, seed):
    #     # Set the random seed for reproducibility
    #     random.seed(seed)

    #     # Wait for the top-level menu elements to be visible
    #     top_level_toc_button_id = # 'li[depth="0"][id^="genToc_"] .toc-text' '[id^="genToc_"]'
    #     # await self.page.get_by_role("button", name="Table of Contents").click()
    #     await self.page.wait_for_selector(top_level_toc_button_id, state='visible')

    #     # Get all elements with IDs starting with "genToc_"
    #     anchor_elements = await self.page.query_selector_all(top_level_toc_button_id)
    #     logger.debug(f"anchor_elements: {anchor_elements}")

    #     if not anchor_elements:
    #         raise Exception("No top-level menu elements found")

    #     # Randomly select one of the top-level elements
    #     selected_element = random.choice(anchor_elements)

    #     # Get the text content of the selected element
    #     element_text = await selected_element.text_content()
        
    #     # Get the href attribute of the selected element
    #     element_href = await selected_element.get_attribute('href')

    #     # Click the selected element
    #     await selected_element.click()

    #     # Log the selection
    #     logger.info(f"Randomly selected top-level menu element: '{selected_element}'\ntext: '{element_text}\nhref: '{element_href}'")

    #     # Return the selected element for further use if needed
    #     return selected_element, element_text, element_href



    # # Example ways to inspect a JSHandle
    # async def inspect_handle(self, idx, handle):
    #     # Get all properties
    #     properties = await handle.evaluate('''node => ({
    #         tagName: node.tagName,
    #         id: node.id,
    #         className: node.className,
    #         textContent: node.textContent,
    #         attributes: Array.from(node.attributes).map(attr => ({
    #             name: attr.name,
    #             value: attr.value
    #         }))
    #     })''')
        
    #     # Or get specific attributes
    #     href = await handle.get_attribute('href')
    #     text = await handle.text_content()
        
    #     logger.info(f"Element {idx}\nProperties: {properties}\nhref: {href}\ntext: {text}")


    async def randomly_select_top_level_menu_element(self, seed):
        """
        Randomly selects and clicks a top-level menu element from a Municode page.
        
        Args:
            seed (int): Random seed for reproducibility
            
        Returns:
            tuple: (selected_element, element_text, element_href)
        """
        # Set the random seed for reproducibility
        random.seed(seed)

        try:
            # Wait for the top-level menu elements to be visible
            # Modified selector to target the actual clickable elements more precisely
            node_id_selector  = 'a[href*="?nodeId="]' # NOTE This selector works.
            await self.page.wait_for_selector(node_id_selector , state='visible', timeout=10000)

            # Get all elements with the modified selector
            anchor_elements  = await self.page.query_selector_all(node_id_selector)
            logger.debug(f"Found {len(anchor_elements)} anchor elements")
            logger.debug(f"anchor_elements: {anchor_elements}")

            if not anchor_elements:
                raise Exception("No anchor elements found")


            # Filter these elements based on the genToc_ regex
            filtered_elements = await self.page.evaluate('''
                (anchors) => {
                    const regex = new RegExp('^genToc_.*');  // Example regex for IDs
                    return anchors.filter(anchor => {
                        // Check if the parent element's ID matches the regex
                        return regex.test(anchor.parentElement.id);
                    }).map(anchor => ({
                        href: anchor.href,
                        text: anchor.textContent,
                        parentId: anchor.parentElement.id
                    }));
                }
            ''', anchor_elements)
            logger.debug(f"filtered_elements len: {len(filtered_elements)}\nfiltered_elements: {filtered_elements}")


            # Recursive clicking.
            clicked_buttons = set()
            growth = 0
            for idx, element in enumerate(filtered_elements, start=1):
                try:
                    click_button_results = await self.page.evaluate('''
                        async (anchors) => {
                            const regex = new RegExp('^genToc_.*');  // Example regex for IDs
                            const clickedButtons = [];
                            
                            for (const anchor of anchors) {
                                if (regex.test(anchor.parentElement.id)) {
                                    // Look for a button within the same parent element
                                    const button = anchor.parentElement.querySelector('button');
                                    if (button) {
                                        await button.click();  // Click the button if found
                                        clickedButtons.push({
                                            href: anchor.href,
                                            parentId: anchor.parentElement.id,
                                            buttonText: button.textContent.trim()
                                        });
                                    }
                                }
                            }
                            return clickedButtons;
                        }
                    ''', element)
                    clicked_buttons.update(click_button_results)
                    growth += len(clicked_buttons)
                    logger.info(f"Sweep {idx} found {growth} buttons. Clicking again...")

            except AsyncPlaywrightTimeoutError as e:
                logger.error(f"Timeout error occurred for click_button_results: {e}")
            # logger.debug(f"click_button_results len: {len(click_button_results)}\nclick_button_results: {click_button_results}", t=30)

            # Read JavaScript from a file
            js_filepath = os.path.join(os.path.dirname(__file__), 'expandAndGather.js')
            with open(js_filepath, 'r') as file:
                expand_and_gather_js = file.read()
            
            # Recursively click all the nodes.
            expand_and_gather_results = await self.page.evaluate(expand_and_gather_js, anchor_elements)
            logger.debug(f"expand_and_gather_results len: {len(expand_and_gather_results)}\expand_and_gather_results: {expand_and_gather_results}", t=30)
            # Once all the icons are clicked, we can just get all the HTML from the page.
            # This should also return how many nodes we clicked
            results = await self.page.inner_html('body')



            # for idx, anchor in enumerate(anchor_elements, start=1):
            #     # Debug: Inspect the first anchor
            #     # element_details = await anchor.evaluate('''anchor => ({
            #     #     tagName: anchor.tagName,
            #     #     href: anchor.href,
            #     #     text: anchor.textContent,
            #     #     className: anchor.className,
            #     #     hasParentLi: !!anchor.closest('li[depth="0"]'),
            #     #     hasButton: !!anchor.closest('li[depth="0"]')?.querySelector('button[role="button"]')
            #     # })''')
            #     element_details = await self.page.evaluate('''
            #         anchor => {
            #             // Initialize details object for the anchor
            #             const details = {
            #                 tagName: anchor.tagName,
            #                 href: anchor.href,
            #                 text: anchor.textContent.trim(),
            #                 className: anchor.className,
            #                 hasParentLi: !!anchor.closest('li[depth="0"]'),
            #                 hasButton: false
            #             };
            #             const regex = new RegExp('^genToc_.*');
            #             const matchingElements = [];
            #                 .forEach(element => {
            #                 if (regex.test(element.id)) {
            #                     const buttons = element.querySelectorAll('button');
            #                     buttons.forEach(button => matchingElements.push(button));
            #                 }
            #             });
                                                           

            #             const li = !!anchor.closest('li[depth="0"]');
            #             if (li) {
            #                 console.log('Found element in li')
            #                 console.log(details)
            #                 details.hasParentLi = true;
            #                 // Look for button within the li
            #                 const button = li.querySelector('.toc-button-expand, button.toggle-node-button');
            #                 if (button) {
            #                     details.hasButton = true;
            #                     details.buttonClass = button.className;
            #                     details.buttonRole = button.getAttribute('role');
            #                 }
            #             }
                        
            #             return details;
            #         }
            #     ''', anchor)
            #     logger.debug(f"{idx} anchor details: {element_details}")
                #await self.inspect_handle(idx, anchor)

            #genToc_TIT1GEPR > button
            #     # Get the containing list item (parent or ancestor with depth="0")
            #     parent_li: bool = await anchor.evaluate('''
            #         anchor => { 
            #             anchor.closest('li[depth="0"]')
            #             return li ? true : false;
            #         }
            #     ''')
            #     logger.info(f"selected_li: {parent_li}")

            #     if not parent_li:
            #         logger.debug(f"Skipping anchor {idx} as it's not a top-level menu item")
            #         continue
            #     else:
            #         # Find the button within the same list item
            #         button_exists = await anchor.evaluate('''
            #             anchor => {
            #                 const li = anchor.closest('li[depth="0"]');
            #                 const button = li.querySelector('button[role="button"]');
            #                 return button ? true : false;
            #             }
            #         ''')

            #         if button_exists:
            #             valid_elements.append(anchor)
            #             logger.error(f"Found button for anchor {idx}")
            #             continue
        
            # logger.info(f"Found {len(valid_elements)} anchor elements in li with a button")
            # output_list = []
            # for element in valid_elements:
            #     element: ElementHandle
            #     logger.info(f"Valid element: {element}")

            #     # Make sure the element is in view before clicking
            #     await element.scroll_into_view_if_needed()
                
            #     # Add a small delay to ensure the element is properly rendered
            #     await self.page.wait_for_timeout(500)

            #     # Click the element and wait for navigation if needed
            #     try:
            #         await element.click()
            #         # Optionally wait for any navigation or content update
            #         await self.page.wait_for_load_state('networkidle', timeout=5000)
            #     except Exception as e:
            #         logger.warning(f"Click failed, attempting JavaScript click: {str(e)}")
            #         await self.page.evaluate('element => element.click()', element)
            #         element_text = await element.text_content()
            #         element_href = await element.get_attribute('href')

            #     logger.info(
            #         f"Successfully selected top-level menu element:\n"
            #         f"Text: '{element_text}'\n"
            #         f"Href: '{element_href}'"
            #     )
            #     output_list.append((element, element_text, element_href))
            # return output_list 

        except Exception as e:
            logger.error(f"Error in randomly_select_top_level_menu_element: {str(e)}")
            raise





class GetMunicodeSidebarElements(AsyncPlaywrightScrapper):
    """
    Get the sidebar elements from a Municode URL page.
    NOTE This uses Playwright rather than Selenium.
    Using a synchronous library to deal with asynchronous JavaScript is more trouble than it's worth.
    Also, fuck multiple libraries.
    """

    def __init__(self,
                domain: str,
                pw_instance,
                *args,
                user_agent: str="*",
                **kwargs):
        super().__init__(domain, pw_instance, *args, user_agent=user_agent, **kwargs)
        self.xpath_dict = {
            "current_version": '//*[@id="codebankToggle"]/button/text()',
            "version_button": '//*[@id="codebankToggle"]/button/',
            "version_text_paths": '//mcc-codebank//ul/li//button/text()',
            'toc': "//input[starts-with(@id, 'genToc_')]" # NOTE toc = Table of Contents
        }
        self.queue = deque()
        self.output_folder:str = output_folder
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

        #         <div class="col-sm-6 hidden-md hidden-lg hidden-xl" style="margin-top: 8px;">
        #     <button type="button" class="btn btn-raised btn-primary" ng-click="$root.zoneMgrSvc.toggleVisibleZone()">
        #         <i class="fa fa-list-ul"></i> Browse table of contents
        #     </button>
        # </div>

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

        # '#codebankToggle button[data-intro="Switch between old and current versions."]' # f'button:has({codebank_label}):has(i.fa fa-caret-down)'

        logger.info("Waiting for the codebank button to be visible...")
        await self.page.wait_for_selector(codebank_button, state='visible')

        logger.info("Codebank button is visible. Getting current version from it...") # CSS selector ftw???
        current_version = await self.page.locator(codebank_button).text_content() #codebank > ul > li:nth-child(1) > div.timeline-entry > div > div
        logger.debug(f"current_version: {current_version}")


        # Hover over and click the codebank to open the popup menu
        logger.info(f"Got current code version '{current_version}'. Hovering over and clicking Codebank button...")
        await self.move_mouse_cursor_to_hover_over(codebank_button)
        await self.page.click(codebank_button)
        
        # Wait for the popup menu to appear
        logger.info("Codebank button was hovered over and clicked successfully. Waiting for popup menu...")
        popup_selector = 'List of previous versions of code'

        # 'aria-label.List of previous versions of code' # NOTE'.popup-menu' is a CSS selector! Also, since the aria-label is hidden, you need to use state='hidden' in order to get it.
        await self.page.wait_for_selector(popup_selector, state='hidden')
        #codebank > ul > li:nth-child(1) > div.timeline-entry > div > div > button
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


    # def _skip_if_we_have_url_already(self, url: str) -> list[dict]|None:
    #     """
    #     Check if we already have a CSV file of the input URL. 
    #     If we do, load it as a list of dictionaries and return it. Else, return None
    #     """
    #     url_file_path = os.path.join(OUTPUT_FOLDER, f"{sanitize_filename(url)}.csv")
    #     if os.path.exists(url_file_path):
    #         logger.info(f"Got URL '{url}' already. Loading csv...")
    #         output_dict = load_from_csv(url_file_path)
    #         return output_dict
    #     else:
    #         return None


    async def get_page_version(self) -> bool:
        return self.is_regular_municode_page()

    # Decorator to wait per Municode's robots.txt
    # NOTE Since code URLs are processed successively, we can subtract off the time it took to get all the pages elements
    # from the wait time specified in robots.txt. This should speed things up (?).

    #@async_adjust_wait_time_for_execution(wait_in_seconds=LEGAL_WEBSITE_DICT["municode"]["wait_in_seconds"])

    @try_except(exception=[AsyncPlaywrightError])
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






        # try: # NOTE get_by commands return a LOCATOR and thus are NOT Coroutines that need to be awaited.

        #     num = await self.page.locator(codebank_button).count()
        #     logger.info(f"Found {num} 'Close' buttons")

        #     # button: Locator = self.page.get_by_label("Table of Contents").get_by_role("button").and_(self.page.get_by_text("Close"))
        #     button: Locator = await self.page.locator(codebank_button)

        #     await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}.html")

        #     #self.page.get_by_test_id("toc").get_by_role("button", name="Close", include_hidden=True) #self.page.wait_for_selector(codebank_button, state="visible", timeout=5000)

        #     logger.info("version menu 'X' button found")
        # except Exception as e:
        #     logger.exception(f"version menu 'X' button not found: {e}")
        #     raise

        # # # Hover over and click the X to open the popup menu
        # # logger.debug(f"Hovering over and pressing version menu 'X' button to close the version menu...")
        # # await element.hover()
        # # #await self.move_mouse_cursor_to_hover_over(codebank_button)
        # logger.debug(f"Hover over version menu 'X' button successful...\nClicking...")
        # await asyncio.sleep(1)
        # await button.click()

        # # logger.debug("Clicking with force=True")
        # # await asyncio.sleep(1)
        # # await button.click(force=True),

        # await self.save_page_html_content_to_output_dir(f"{self.place_name}_{prefix}_after_click.html")

        # # JavaScript click
        # logger.debug("Clicking with JS")
        # await asyncio.sleep(1)
        # await button.evaluate('element => element.click()'),
        

        # # Dispatch click event
        # logger.debug("Clicking with dispatch event click")
        # await asyncio.sleep(1)
        # await button.dispatch_event('click'),

        # # Double click
        # logger.debug("Double Clicking")
        # await asyncio.sleep(1)
        # await button.dblclick(),

        # # Click with delay
        # logger.debug("Clicking with delay")
        # await asyncio.sleep(1)
        # await button.click(delay=100),

        # # if await self.page.get_by_role("button").and_(self.page.get_by_text("Close")).count() > 0:
        # #     logger.error(f"version menu 'X' button still visible after clicking. Clicking again...")
        # #     await self.page.get_by_role("button").and_(self.page.get_by_text("Close")).click()

        # logger.debug(f"Version menu 'X' button clicked successfully.\nReturning...")

        # return


    # async def debug_button_click(self, button: Locator):

    #     # 1. Basic element checks
    #     logger.info("=== BASIC ELEMENT CHECKS ===")
    #     count = await button.count()
    #     logger.info(f"Elements found: {count}")
        
    #     if count == 0:
    #         logger.error("Button not found in DOM!")
    #         return
            
    #     # 2. Visibility checks
    #     logger.info("\n=== VISIBILITY CHECKS ===")
    #     is_visible = await button.is_visible()
    #     is_hidden = await button.is_hidden()
    #     logger.info(f"Is visible: {is_visible}")
    #     logger.info(f"Is hidden: {is_hidden}")
        
    #     # 3. Element properties
    #     logger.info("\n=== ELEMENT PROPERTIES ===")
    #     properties = await self.page.evaluate('''element => {
    #         const computedStyle = window.getComputedStyle(element);
    #         return {
    #             display: computedStyle.display,
    #             visibility: computedStyle.visibility,
    #             opacity: computedStyle.opacity,
    #             position: computedStyle.position,
    #             zIndex: computedStyle.zIndex,
    #             pointerEvents: computedStyle.pointerEvents,
    #             disabled: element.disabled,
    #             offsetWidth: element.offsetWidth,
    #             offsetHeight: element.offsetHeight,
    #             getBoundingClientRect: element.getBoundingClientRect()
    #         }
    #     }''')
    #     logger.info(f"Element properties: {properties}")
        
    #     # 4. Check event listeners
    #     logger.info("\n=== EVENT LISTENERS ===")
    #     has_listeners = await button.evaluate('''element => {
    #         const listeners = window.getEventListeners ? window.getEventListeners(element) : {};
    #         return {
    #             hasClickListener: 'click' in listeners,
    #             totalListeners: Object.keys(listeners).length,
    #             listenerTypes: Object.keys(listeners)
    #         }
    #     }''')
    #     logger.info(f"Event listeners: {has_listeners}")
        
    #     # 5. Set up console monitoring
    #     logger.info("\n=== ATTEMPTING CLICKS ===")
    #     self.page.on('console', lambda msg: logger.debug(f'Console: {msg.text}'))
    #     self.page.on('pageerror', lambda err: logger.error(f'Page error: {err.text}'))
        
    #     # 6. Try different click methods

    #     click_attempts = [
    #         # Force click
    #         await button.click(force=True),

    #         # JavaScript click
    #         await button.evaluate('element => element.click()'),
            
    #         # Dispatch click event
    #         await button.dispatch_event('click'),

    #         # Double click
    #         await button.dblclick(),

    #         # Click with delay
    #         await button.click(delay=100),

    #     ]

    #     # Try each click method
    #     for i, click_attempt in enumerate(click_attempts, 1):
    #         try:
    #             logger.info(f"\nTrying click method {i}...")
    #             await click_attempt()
    #             logger.info(f"Click method {i} completed without errors")

    #             # Check if element still exists after click
    #             post_click_count = await button.count()
    #             logger.info(f"Element still exists after click: {post_click_count > 0}")

    #             # Brief pause to observe any changes
    #             await asyncio.sleep(0.5)
                
    #         except Exception as e:
    #             logger.error(f"Click method {i} failed: {str(e)}")
        
    #     # 7. Check for overlapping elements
    #     logger.info("\n=== CHECKING FOR OVERLAPPING ELEMENTS ===")
    #     overlapping = None #or await self.page.evaluate('''() => {
    #     #     const element = document.querySelector('#toc button[aria-label="Close"]');
    #     #     if (!element) return [];
            
    #     #     const rect = element.getBoundingClientRect();
    #     #     const elements = document.elementsFromPoint(
    #     #         rect.left + rect.width/2,
    #     #         rect.top + rect.height/2
    #     #     );
    #     #     return elements.map(el => ({
    #     #         tag: el.tagName,
    #     #         id: el.id,
    #     #         class: el.className,
    #     #         zIndex: window.getComputedStyle(el).zIndex
    #     #     }));
    #     # }''')
    #     logger.info(f"Elements at click position: {overlapping}")
        
    #     # 8. Final element state
    #     logger.info("\n=== FINAL ELEMENT STATE ===")
    #     final_count = await button.count()
    #     final_visible = await button.is_visible() if final_count > 0 else False
    #     logger.info(f"Element still exists: {final_count > 0}")
    #     logger.info(f"Element still visible: {final_visible}")
        
    #     return {
    #         "found": count > 0,
    #         "visible": is_visible,
    #         "properties": properties,
    #         "has_listeners": has_listeners,
    #         "overlapping_elements": overlapping
    #     }


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
    
    logger.info("get_sidebar_urls_from_municode_with_selenium function complete. Making dataframes and saving...")
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
    # Turn the list of dicts into a dataframe.
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


from utils.shared.save_to_csv import save_to_csv
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

    # Turn the list of dicts into a dataframe.
    code_versions_df = pd.DataFrame.from_records(output_list)

    # Drop the urls columns.
    code_versions_df.drop(['url_hash','table_of_contents_urls'], axis=1, inplace=True)

    # Save the dataframe to the output folder.
    output_file = os.path.join(output_folder, sanitize_filename(output_list[0]['input_url']))
    save_to_csv(code_versions_df, output_file)

    return



    # def _get_current_code_version(self, selector: str) -> str:
    #     """
    #     Get the date for the current version of the municipal code.
    #     """

    #     # Wait for the button to be visible
    #     button_selector = 'button:has(span.text-xs.text-muted):has(i.fa.fa-caret-down)'
    #     self.page.wait_for_selector(button_selector)

    #     # Initialize HTML targets and JavaScript command.
    #     version_date_id = 'span.text-sm.text-muted'
    #     args = {"version_date_id": version_date_id}
    #     js = '() => document.querySelector("{version_date_id}").textContent'

    #     # Wait for the element to be visible
    #     self.page.wait_for_selector(version_date_id)

    #     # Get the code with JavaScript
    #     version_date: str = self.evaluate_js(js, js_kwargs=args)

    #     logger.debug(f'version_date: {version_date}')
    #     return version_date.strip()


    # async def _get_all_code_versions(self, url: str) -> list[str]:
    #     """
    #     Get the dates for current and past versions of the municipal code.
    #     NOTE: You need to click on each individual button to get the link itself.
    #     """
    #     version_date_button_selector = 'span.text-sm.text-muted'

    #     version_date = self._get_current_code_version()


    #     # Press the button that shows the code archives pop-up
    #     version_button = None
    #     await self.click_on(version_button)
    #     self.press_buttons(url, xpath=self.xpath_dict['version_button'])

    #     # Get all the dates in the pop-up.
    #     buttons = self.wait_for_and_then_return_elements(
    #         self.xpath_dict['version_text_paths'], wait_time=10, poll_frequency=0.5
    #     )
    #     version_list = [
    #         button.text.strip() for button in buttons
    #     ]
    #     logger.debug(f'version_list\n{version_list}',f=True)
    #     return version_list
