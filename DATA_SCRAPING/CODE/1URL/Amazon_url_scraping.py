
"""Code for url scraping from amazon.ae"""
"""
This module automates searching product names on amazon.ae, extracts the top 5 search-result URLs
and titles for each product, and persists the results into a JSON file and an SQLite database.
It uses Playwright (Firefox) for browser automation, BeautifulSoup for HTML parsing, and a
simple retry/delay strategy to reduce detection. The module also provides small helper functions
for JSON persistence and HTML element-safe text extraction.
Primary behavior
----------------
- Reads input product list from a CSV file (expected columns: "SL NO", "Item Name").
- For each product (row) not already present in the JSON output file:
    - Opens amazon.ae in a Playwright-controlled Firefox browser.
    - Performs a search for the product name.
    - Parses the search results page HTML with BeautifulSoup.
    - Extracts up to five product result URLs and combined brand+title texts.
    - Writes/merges results into a JSON file and inserts a row into an SQLite table.
    - Logs progress, warnings and errors to a rotating log file, and uses random delays between actions.


Notes
-----
- File paths and 'headless' behavior are module constants and can be adjusted for different
    environments (development vs. CI).
- The HTML selectors used are based on the observed structure of amazon.ae search results at
    implementation time and may require updates if the site's markup changes."""



# IMPORTS 
import sqlite3
import json
import logging
import time
import random
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
import os


# CONFIGURATION
BASE_URL = "https://www.amazon.ae/"
CSV_FILE = "/home/anusha/Desktop/Servoo/Files/Amal Trading - Sheet1 (2).csv"
JSON_FILE = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/DATA/top_product_urls.json"
LOG_FILE = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/LOG/Url_scraper_amazon.log"
DB_FILE = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/DATA/Url_output_amazon.db"



# LOGGING 
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.info("Amazon.ae URL Scraper started.")



# HEADERS 
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/118.0.5993.90 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;"
              "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}



# DATABASE 
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_product_urls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        serial_number TEXT,
        input_title TEXT,
        url_1 TEXT, url_1_title TEXT,
        url_2 TEXT, url_2_title TEXT,
        url_3 TEXT, url_3_title TEXT,
        url_4 TEXT, url_4_title TEXT,
        url_5 TEXT, url_5_title TEXT,
        scraped_date TEXT
    )
""")
conn.commit()



""" Configuration (module-level constants)
--------------------------------------
BASE_URL: str
        Root URL for amazon.ae used for navigation.
CSV_FILE: str
        Path to the input CSV file that contains product serial numbers and item names.
JSON_FILE: str
        Path to the JSON output file where scraped results are stored. The loader robustly handles
        empty or corrupted JSON by creating a backup and starting fresh.
LOG_FILE: str
        Path to the log file used by Python's logging module.
DB_FILE: str
        Path to the SQLite database file. The module ensures the necessary table exists.
Logging and Persistence
-----------------------
- Logging uses logging.basicConfig with timestamps; logs are written to LOG_FILE.
- JSON persistence:
    - load_json(): safely reads JSON_FILE and returns a dict. If JSON is corrupted, the file is
        renamed with a ".backup" suffix and an empty dict is returned.
    - save_json(data): merges 'data' (a mapping of serial_number -> record) into the existing JSON
        contents and writes the combined dictionary back to JSON_FILE with pretty formatting.
- SQLite persistence:
    - The module ensures the table `top_product_urls` exists with columns for serial number,
        input title, up to five URLs and their titles, and scraped_date. Each scraped item is inserted."""




# HELPER FUNCTIONS 
def load_json():
    if os.path.exists(JSON_FILE):
        try:
            with open(JSON_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        except json.JSONDecodeError:
            logging.warning(f"Corrupted JSON file detected. Resetting: {JSON_FILE}")
            backup = JSON_FILE + ".backup"
            os.rename(JSON_FILE, backup)
            logging.info(f"Backup created: {backup}")
    return {}



# json saving function
def save_json(data):
    existing = load_json()
    for sn, value in data.items():
        existing[sn] = value
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=4)



# random delay function
def random_delay(a=2, b=5):
    time.sleep(random.uniform(a, b))



# safe text extraction
def safe_get_text(element):
    return element.get_text(strip=True) if element else "Not Available"




# extract brand and title
def extract_brand_and_title(product_soup):
    """
    Extract brand and product title from a single search result element.
    Brand: <h2 class="a-size-mini s-line-clamp-1"><span>Brand Name</span></h2>
    Title: <h2 aria-label="Product Name"><span>Product Name</span></h2>
    """
    brand_tag = product_soup.select_one("h2.a-size-mini span.a-size-base-plus.a-color-base")
    title_tag = product_soup.select_one("h2[aria-label] span")
    
    brand_text = safe_get_text(brand_tag)
    title_text = safe_get_text(title_tag)
    
    # Combine brand + title if brand exists
    if brand_text != "Not Available" and brand_text.strip() != "":
        return f"{brand_text.strip()} {title_text.strip()}"
    return title_text.strip()


""" 
Helper functions
----------------
load_json() -> dict
        Load and return the existing contents of JSON_FILE as a dictionary.
        - If the file does not exist or is empty, returns an empty dict.
        - If the file contains invalid JSON, renames the corrupted file to JSON_FILE + ".backup",
            logs a warning, and returns an empty dict.
save_json(data: dict) -> None
        Merge the provided `data` dictionary into the existing JSON_FILE content and write back
        the merged dictionary to disk. Keys are serial numbers (strings).
random_delay(a: float = 2, b: float = 5) -> None
        Sleep for a random duration between `a` and `b` seconds (uniform distribution).
        Used to introduce human-like delays between requests.
safe_get_text(element) -> str
        Given a BeautifulSoup Tag or None, return its stripped text content, or the string
        "Not Available" if the element is None.
extract_brand_and_title(product_soup: bs4.element.Tag) -> str
        Attempts to extract a combined "brand + title" or fallback title from a single search-result
        element (a BeautifulSoup Tag representing an Amazon search result).
        - Looks up a brand-specific selector and a title selector, sanitizes text, and returns:
                - "Brand Title" if brand is present and non-empty
                - "Title" if no brand found
        - Always returns a stripped string; does not return None."""



# MAIN SCRAPER 
df = pd.read_csv(CSV_FILE)
existing_json = load_json()
results = {}

with sync_playwright() as p:
    browser = p.firefox.launch(headless=False)
    context = browser.new_context(extra_http_headers=headers)
    page = context.new_page()
    stealth_sync(page)

    try:
        page.goto(BASE_URL, timeout=60000)
        page.wait_for_load_state("domcontentloaded", timeout=60000)
        logging.info(f"Loaded base URL: {BASE_URL}")
    except Exception as e:
        logging.error(f"Failed to load Amazon.ae home page: {e}")

    for _, row in df.iterrows():
        serial_number = str(row["SL NO"]).strip()
        input_title = str(row["Item Name"]).strip()

        if serial_number in existing_json:
            logging.info(f"Skipping {input_title} (already scraped)")
            continue

        logging.info(f"Searching for: {input_title} (SL NO: {serial_number})")

        try:
            for attempt in range(3):
                try:
                    page.goto(BASE_URL, timeout=60000)
                    page.wait_for_load_state("domcontentloaded", timeout=60000)

                    search_box = page.wait_for_selector("#twotabsearchtextbox", timeout=40000)
                    search_box.fill("")
                    search_box.type(input_title)
                    search_box.press("Enter")

                    page.wait_for_load_state("networkidle", timeout=60000)
                    random_delay(2, 4)
                    break
                except Exception as e:
                    logging.warning(f"Search attempt {attempt+1} failed for {input_title}: {e}")
                    if attempt == 2:
                        raise
                    random_delay(3, 6)

            soup = BeautifulSoup(page.content(), "html.parser")
            product_links = soup.select("div.s-main-slot div[data-component-type='s-search-result']")

            top_urls, top_titles = [], []

            if not product_links:
                logging.warning(f"No results found for {input_title}")
            else:
                for link in product_links[:5]:
                    a_tag = link.select_one("a.a-link-normal.s-line-clamp-4.s-link-style.a-text-normal")
                    href = a_tag.get("href") if a_tag else None
                    full_url = "https://www.amazon.ae" + href if href else "Not Available"

                    final_title = extract_brand_and_title(link)
                    top_urls.append(full_url)
                    top_titles.append(final_title)

            # Fill missing entries
            while len(top_urls) < 5:
                top_urls.append("Not Available")
                top_titles.append("Not Available")

            results[serial_number] = {
                "Serial_Number": serial_number,
                "input_title": input_title,
                "1st_url": top_urls[0], "1st_url_title": top_titles[0],
                "2nd_url": top_urls[1], "2nd_url_title": top_titles[1],
                "3rd_url": top_urls[2], "3rd_url_title": top_titles[2],
                "4th_url": top_urls[3], "4th_url_title": top_titles[3],
                "5th_url": top_urls[4], "5th_url_title": top_titles[4],
            }

            save_json({serial_number: results[serial_number]})
            cursor.execute("""
                INSERT INTO top_product_urls (
                    serial_number, input_title,
                    url_1, url_1_title,
                    url_2, url_2_title,
                    url_3, url_3_title,
                    url_4, url_4_title,
                    url_5, url_5_title,
                    scraped_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                serial_number, input_title,
                top_urls[0], top_titles[0],
                top_urls[1], top_titles[1],
                top_urls[2], top_titles[2],
                top_urls[3], top_titles[3],
                top_urls[4], top_titles[4],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
            conn.commit()

            logging.info(f"✅ Scraped top 5 URLs for: {input_title}")
            random_delay(4, 7)

        except Exception as e:
            logging.error(f"❌ Error scraping {input_title}: {e}")
            results[serial_number] = {"Serial_Number": serial_number, "input_title": input_title}
            save_json({serial_number: results[serial_number]})
            conn.commit()
            random_delay(4, 6)

    browser.close()


""" 
Main scraping flow (high-level)
------------------------------
- Loads the input CSV using pandas.
- Loads any pre-existing results from JSON_FILE to skip already-scraped serial numbers.
- Launches a Playwright Firefox browser (headless flag can be adjusted), creates a browser
    context with custom HTTP headers, and applies stealth techniques via `stealth_sync`.
- For each row in the CSV:
    - Skip if serial number already in JSON (prevents re-scraping).
    - Attempt the search up to 3 times (with delays between retries):
        - Navigate to BASE_URL, locate the search box, clear it, type the item name and submit.
        - Wait for network idle and parse the page with BeautifulSoup.
    - Parse up to five search-result elements:
        - Extract the product link href and form a full URL (prefixed with "https://www.amazon.ae" when needed).
        - Use extract_brand_and_title to get a user-friendly title for each result.
        - Fill missing entries with "Not Available" so results always have five entries.
    - Persist each result immediately:
        - Merge into JSON output via save_json.
        - Insert a row into the SQLite `top_product_urls` table with the current timestamp.
    - Use random_delay between items to reduce request rate and mimic human behavior.
- On any exception during an item scrape:
    - Log the error, store a minimal record in JSON noting the failure, and continue to next item.


Error handling and robustness
-----------------------------
- The code uses try/except around navigation and scraping logic to catch and log exceptions,
    attempts retries for searches, and ensures partial results are committed to JSON/DB when errors occur.
- load_json handles corrupted JSON by backing up the bad file to prevent crashes.
- The module commits to the SQLite database after each insert to minimize data loss on failure.
Dependencies
------------
- Python packages: pandas, beautifulsoup4, playwright, playwright-stealth (playwright_stealth),
    sqlite3 (stdlib), json (stdlib), logging (stdlib), time, random, datetime, os.
- Playwright browser binaries must be installed and available (e.g., `playwright install`).
- The CSV input must contain headers "SL NO" and "Item Name".
Security and etiquette
----------------------
- The script sets a realistic User-Agent and custom Accept headers, but scraping policies
    vary by site. Ensure you comply with Amazon's Terms of Service and robots.txt before running
    automated scraping at scale.
- The script uses random delays and limited retries but is not a substitute for respectful rate
    limiting and official APIs."""


# CLEANUP    
conn.close()
logging.info("✅ Amazon.ae URL Scraper finished successfully.")
