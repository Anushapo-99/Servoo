# --------------------------- IMPORTS --------------------------- #
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

# --------------------------- CONFIG --------------------------- #
BASE_URL = "https://www.amazon.ae/"
CSV_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/common_files/Amal Trading - Sheet1 (2).csv"
JSON_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/DATA/Url_output_amaz.json"
LOG_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/LOG/Url_scraper_amaz.log"
DB_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/DATA/product_amaz.db"

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
logging.info("Amazon.ae Scraper started.")

# --------------------------- DATABASE --------------------------- #
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_product_urls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        serial_number TEXT,  
        input_title TEXT,
        url_1 TEXT,
        url_1_title TEXT,
        url_2 TEXT,
        url_2_title TEXT,
        url_3 TEXT,
        url_3_title TEXT,
        url_4 TEXT,
        url_4_title TEXT,
        url_5 TEXT,
        url_5_title TEXT,
        scraped_date TEXT
    )
""")
conn.commit()

# --------------------------- HELPER FUNCTIONS --------------------------- #
def load_json():
    if os.path.exists(JSON_FILE):
        try:
            with open(JSON_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        except json.JSONDecodeError:
            logging.warning(f"Corrupted JSON file detected. Resetting: {JSON_FILE}")
            backup_file = JSON_FILE + ".backup"
            os.rename(JSON_FILE, backup_file)
            logging.info(f"Backup created: {backup_file}")
    return {}

def save_json(data):
    existing = load_json()
    for serial_number, value in data.items():
        if serial_number not in existing:
            existing[serial_number] = value
        else:
            logging.info(f"Duplicate skipped in JSON for serial_number {serial_number}")
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=4)

def random_delay(min_sec=2, max_sec=5):
    time.sleep(random.uniform(min_sec, max_sec))

def scrape_title(page, url):
    for attempt in range(3):
        try:
            page.goto(url, timeout=45000)
            page.wait_for_load_state("networkidle", timeout=20000)
            random_delay(2, 3)
            soup = BeautifulSoup(page.content(), "html.parser")
            title_h2 = soup.select_one(
                "h2.a-size-base-plus.a-spacing-none.a-color-base.a-text-normal span"
            )
            if title_h2:
                return title_h2.get_text(strip=True)
        except Exception as e:
            logging.warning(f"Retry {attempt+1}/3 failed for title at {url}: {e}")
            random_delay(3, 5)
    return "Not Available"

# --------------------------- MAIN SCRAPER --------------------------- #
df = pd.read_csv(CSV_FILE)
existing_json = load_json()
results = {}

with sync_playwright() as p:
    browser = p.firefox.launch(headless=False)
    context = browser.new_context(extra_http_headers=headers)
    page = context.new_page()
    stealth_sync(page)
    
    # page.goto(BASE_URL, timeout=60000)
    # page.wait_for_load_state("networkidle")
    page.goto(BASE_URL, timeout=60000)
    page.wait_for_load_state("domcontentloaded", timeout=60000)

    logging.info(f"Loaded base URL: {BASE_URL}")

    for index, row in df.iterrows():
        serial_number = str(row['SL NO']).strip()
        input_title = str(row['Item Name']).strip()

        if serial_number in existing_json:
            logging.info(f"Skipping {input_title} (already scraped)")
            continue
        
        logging.info(f"Searching for product: {input_title} (SL NO: {serial_number})")
        try:
            # 🔹 Search with retries
            for attempt in range(3):
                try:
                    search_input = page.wait_for_selector("#twotabsearchtextbox", timeout=50000)
                    search_input.fill("")
                    search_input.type(input_title)
                    random_delay(1.5, 3)
                    search_input.press("Enter")
                    page.wait_for_load_state("networkidle", timeout=60000)
                    random_delay(2, 4)
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    logging.warning(f"Retry {attempt+1}/3 failed during search: {e}")
                    random_delay(3, 5)

            # 🔹 Extract product URLs and titles
            soup = BeautifulSoup(page.content(), "html.parser")
            product_links = soup.select("a.a-link-normal.s-line-clamp-4.s-link-style.a-text-normal")

            top5_urls = []
            top5_titles = []

            for link in product_links[:5]:
                url = link.get('href')
                if url and not url.startswith("http"):
                    url = "https://www.amazon.ae" + url
                top5_urls.append(url)
                
                title = scrape_title(page, url) if url else "Not Available"
                top5_titles.append(title)

            while len(top5_urls) < 5:
                top5_urls.append("Not Available")
                top5_titles.append("Not Available")

            results[serial_number] = {
                "Serial_Number": serial_number,
                "input_title": input_title,
                "1st_url": top5_urls[0],
                "1st_url_title": top5_titles[0],
                "2nd_url": top5_urls[1],
                "2nd_url_title": top5_titles[1],
                "3rd_url": top5_urls[2],
                "3rd_url_title": top5_titles[2],
                "4th_url": top5_urls[3],
                "4th_url_title": top5_titles[3],
                "5th_url": top5_urls[4],
                "5th_url_title": top5_titles[4],
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
                top5_urls[0], top5_titles[0],
                top5_urls[1], top5_titles[1],
                top5_urls[2], top5_titles[2],
                top5_urls[3], top5_titles[3],
                top5_urls[4], top5_titles[4],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            conn.commit()

            logging.info(f"Scraped URLs and titles for: {input_title}")
            random_delay(3, 6)
            page.goto(BASE_URL, timeout=60000)
            page.wait_for_load_state("networkidle")
            random_delay(2, 4)

        except Exception as e:
            logging.error(f"Error scraping {input_title}: {e}")
            results[serial_number] = {
                "Serial_Number": serial_number,
                "input_title": input_title,
                "1st_url": "Not Available",
                "1st_url_title": "Not Available",
                "2nd_url": "Not Available",
                "2nd_url_title": "Not Available",
                "3rd_url": "Not Available",
                "3rd_url_title": "Not Available",
                "4th_url": "Not Available",
                "4th_url_title": "Not Available",
                "5th_url": "Not Available",
                "5th_url_title": "Not Available",
            }
            save_json({serial_number: results[serial_number]})

    browser.close()
conn.close()
logging.info("Amazon.ae Scraper finished successfully.")
