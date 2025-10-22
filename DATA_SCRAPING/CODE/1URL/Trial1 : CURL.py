# amazon_curl_cffi_search_scraper.py
# Converted to use curl_cffi + BeautifulSoup for Amazon.ae search pages

import sqlite3
import json
import logging
import time
import random
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests
import os
import urllib.parse

# --------------------------- CONFIG --------------------------- #
BASE_URL = "https://www.amazon.ae/"
CSV_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/common_files/Amal Trading - Sheet1 (2).csv"
JSON_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/DATA/Url_output_amazon.json"
LOG_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/LOG/Url_scraper_amazon.log"
DB_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/DATA/Url_output_amazon.db"

PROXY = None

# --------------------------- HEADERS --------------------------- #
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

# --------------------------- LOGGING --------------------------- #
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.info("Amazon.ae (curl_cffi) URL Scraper started.")

# --------------------------- DATABASE --------------------------- #
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

# --------------------------- HELPERS --------------------------- #
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

def save_json(data):
    existing = load_json()
    for sn, value in data.items():
        existing[sn] = value
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=4)

def random_delay(a=2, b=5):
    time.sleep(random.uniform(a, b))

def safe_get_text(element):
    return element.get_text(strip=True) if element else "Not Available"

def extract_brand_and_title(product_soup):
    """
    Extract brand and product title from a single search result element.
    This uses the same logic as your original function but is tolerant of missing tags.
    """
    brand_tag = product_soup.select_one("h2.a-size-mini span.a-size-base-plus.a-color-base")
    title_tag = product_soup.select_one("h2[aria-label] span") or product_soup.select_one("h2 a span")
    
    brand_text = safe_get_text(brand_tag)
    title_text = safe_get_text(title_tag)
    
    if brand_text != "Not Available" and brand_text.strip() != "":
        return f"{brand_text.strip()} {title_text.strip()}"
    return title_text.strip()

def is_captcha_or_block(response_text):
    # Basic checks for common robot/captcha pages - extend as needed
    lower = response_text.lower()
    if "robot check" in lower or "captcha" in lower or "enter the characters you see below" in lower:
        return True
    if "we have detected unusual traffic" in lower or "are you a human" in lower:
        return True
    return False

# --------------------------- CURL_CFFI REQUEST FUNCTION --------------------------- #
def fetch_search_page(query, attempt=1, timeout=30):
    """
    Fetch search results page for given query using curl_cffi.
    Uses impersonate to mimic a modern Chrome browser.
    """
    encoded_q = urllib.parse.quote_plus(query)
    search_url = f"{BASE_URL.rstrip('/')}/s?k={encoded_q}"
    opts = {
        "timeout": timeout,
        "headers": headers,
        "impersonate": "chrome124",
    }
    if PROXY:
        opts["proxies"] = {"https": PROXY, "http": PROXY}
    try:
        resp = requests.get(search_url, **opts)
        status = getattr(resp, "status_code", None)
        text = resp.text if hasattr(resp, "text") else resp.content.decode("utf-8", errors="replace")
        return status, text
    except Exception as e:
        logging.warning(f"fetch_search_page attempt {attempt} failed for '{query}': {e}")
        return None, None

# --------------------------- MAIN SCRAPER --------------------------- #
df = pd.read_csv(CSV_FILE)
existing_json = load_json()
results = {}

for _, row in df.iterrows():
    serial_number = str(row.get("SL NO", "")).strip()
    input_title = str(row.get("Item Name", "")).strip()

    if not serial_number:
        # skip rows without serial number
        continue

    if serial_number in existing_json:
        logging.info(f"Skipping {input_title} (already scraped)")
        continue

    logging.info(f"Searching for: {input_title} (SL NO: {serial_number})")

    success = False
    status = None
    page_text = None
    for attempt in range(1, 4):
        status, page_text = fetch_search_page(input_title, attempt=attempt)
        if status is None:
            random_delay(2, 5)
            continue

        # basic anti-bot / captcha detection
        if is_captcha_or_block(page_text):
            logging.warning(f"Captcha/Block detected for '{input_title}' (attempt {attempt}, status={status}).")
            # Wait longer and retry; if you have proxy rotation, rotate here.
            random_delay(10, 20)
            continue

        # successful HTML fetch
        success = True
        break

    if not success:
        logging.error(f"Failed to fetch search results for {input_title} after retries.")
        results[serial_number] = {"Serial_Number": serial_number, "input_title": input_title}
        save_json({serial_number: results[serial_number]})
        # commit a minimal row to DB to mark attempted (optional)
        cursor.execute("""
            INSERT INTO top_product_urls (
                serial_number, input_title, url_1, url_1_title, url_2, url_2_title,
                url_3, url_3_title, url_4, url_4_title, url_5, url_5_title, scraped_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            serial_number, input_title,
            "Not Available", "Not Available",
            "Not Available", "Not Available",
            "Not Available", "Not Available",
            "Not Available", "Not Available",
            "Not Available", "Not Available",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
        conn.commit()
        random_delay(4, 7)
        continue

    # Parse the page with BeautifulSoup
    soup = BeautifulSoup(page_text, "html.parser")
    product_links = soup.select("div.s-main-slot div[data-component-type='s-search-result']")

    top_urls, top_titles = [], []

    if not product_links:
        logging.warning(f"No results found for {input_title} (status={status})")
    else:
        for link in product_links[:5]:
            # anchor tags for product result
            a_tag = link.select_one("a.a-link-normal.s-link-style.a-text-normal") or link.select_one("h2 a")
            href = a_tag.get("href") if a_tag else None
            if href and href.startswith("/"):
                full_url = urllib.parse.urljoin(BASE_URL, href.split("?")[0])
            elif href:
                full_url = href
            else:
                full_url = "Not Available"

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
    random_delay(4, 8)

# cleanup
conn.close()
logging.info("✅ Amazon.ae (curl_cffi) URL Scraper finished successfully.")
