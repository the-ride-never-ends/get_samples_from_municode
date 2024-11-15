from playwright.sync_api import sync_playwright
import time
import re
import os
import sys


# Get the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory (one layer higher)
parent_dir = os.path.dirname(current_dir)

# Insert the parent directory at the beginning of sys.path
sys.path.insert(0, parent_dir)

from config.config import OUTPUT_FOLDER

from logger.logger import Logger
logger = Logger(logger_name=__name__)

from utils.shared.sanitize_filename import sanitize_filename

def screenshot_urls_with_commas_in_them(url: str):
    with sync_playwright() as p:
        # Launch browser with specific options to avoid detection
        browser = p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )

        # Create a context with specific options
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )

        # Create a new page
        page = context.new_page()
        
        # Add extra headers
        page.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })

        try:
            # Navigate to the page with a timeout
            page.goto(url, wait_until='networkidle', timeout=30000)

            # Add a small delay to ensure dynamic content loads
            time.sleep(10)

            # Screenshot the page
            screenshot_path = os.path.join(OUTPUT_FOLDER, f"{sanitize_filename(url)}_debug.jpg")
            page.screenshot(path=screenshot_path, type="jpeg", quality=80)

            return

        except Exception as e:
            logger.error(f"Error accessing {url}: {e}")
            return None
        
        finally:
            browser.close()

def clean_municode_url(url: str):
    """Clean and normalize the Municode URL"""
    # Remove any URL encoding
    url = url.replace('%2C', ',').replace('%20', ' ')
    
    # Ensure proper format for township URLs
    if '(' in url and ')' in url:
        # Extract township and county names
        match = re.search(r'([^/]+)_township.*\((.*?)\)', url)
        if match:
            township, county = match.groups()
            # Reconstruct the URL with proper formatting
            base_url = url.split('/mi/')[0] + '/mi/'
            return f"{base_url}{township.strip()}_township,_({county.strip().lower()}_co.)"
    
    return url

def main():
    urls = [
        "https://library.municode.com/mi/bath_charter_township%2C_(clinton_co.)",
        "https://library.municode.com/mi/baldwin_township,_(iosco_co.)"
    ]
    
    for url in urls:
        clean_url = clean_municode_url(url)
        logger.debug(f"\nProcessing: {clean_url}")
        screenshot_urls_with_commas_in_them(clean_url)

if __name__ == "__main__":
    main()