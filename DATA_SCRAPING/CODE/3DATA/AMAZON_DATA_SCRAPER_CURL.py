# AMAZON DATA SCRAPER USING CURL

import sqlite3
import time
import random
import csv
import os
import logging
from datetime import datetime
from tqdm import tqdm
from curl_cffi import requests
from bs4 import BeautifulSoup

# ------------------ USER CONFIG ------------------
DB_FILE = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/DATA/Url_output_amazon.db"
USER_AGENTS_FILE = "/home/anusha/Desktop/DATAHUT/Macys_clothing/user_agents.txt"
OUTPUT_CSV = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/DATA/Servoo_Scraped_Data.csv"
OUTPUT_TABLE = "scraped_products"

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

MIN_DELAY = 2.0
MAX_DELAY = 6.0

SOURCE_WEBSITE = "https://www.amazon.ae/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ------------------ HELPERS ------------------
def load_user_agents(path):
    if not os.path.exists(path):
        logging.error("User agents file not found: %s", path)
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return [l.strip() for l in f if l.strip()]

def ensure_scraped_column(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(url_similarity)")
    cols = [r[1] for r in cur.fetchall()]
    if "scraped" not in cols:
        logging.info("Adding 'scraped' column to url_similarity table")
        cur.execute("ALTER TABLE url_similarity ADD COLUMN scraped INTEGER DEFAULT 0")
        conn.commit()

def prepare_output_table(conn):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
            Scrape_ID INTEGER,
            Serial_Number TEXT,
            Product_Name TEXT,
            Matched_Product_Name TEXT,
            Description TEXT,
            Price_AED TEXT,
            Image_URL TEXT,
            Barcode TEXT,
            Source_URL TEXT,
            Source_Website TEXT,
            Last_Updated TEXT
        )
    """)
    conn.commit()

def append_to_csv(row, csv_path=OUTPUT_CSV):
    header = ["Scrape_ID","Serial_Number","Product_Name","Matched_Product_Name",
              "Description","Price (AED)","Image_URL","Barcode","Source_URL","Source_Website","Last_Updated"]
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerow(row)

def insert_output_table(conn, row):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {OUTPUT_TABLE} (
            Scrape_ID, Serial_Number, Product_Name, Matched_Product_Name,
            Description, Price_AED, Image_URL, Barcode, Source_URL, Source_Website, Last_Updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)
    conn.commit()

def fetch_all_rows(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, serial_number, input_title, matched_url, status
        FROM url_similarity
        WHERE IFNULL(scraped,0) = 0
    """)
    return cur.fetchall()

def mark_scraped(conn, scrape_id):
    cur = conn.cursor()
    cur.execute("UPDATE url_similarity SET scraped = 1 WHERE id = ?", (scrape_id,))
    conn.commit()

def get_page(url, headers, timeout=3000):
    try:
        return requests.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        logging.exception("HTTP error for %s: %s", url, e)
        return None

def extract_title(soup):
    t = soup.select_one("#productTitle")
    return " ".join(t.get_text(strip=True).split()) if t else ""

def extract_price(soup):
    selectors = ["span#priceblock_ourprice","span#priceblock_dealprice",
                 "span.a-price span.a-offscreen","span.a-offscreen"]
    for sel in selectors:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(strip=True)
    return ""

def extract_image(soup):
    img = soup.select_one("#imgTagWrapperId img#landingImage")
    if img and img.get("data-old-hires"):
        return img.get("data-old-hires")
    if img and img.get("src"):
        return img.get("src")
    meta = soup.find("meta", {"property": "og:image"})
    if meta and meta.get("content"):
        return meta.get("content")
    return ""

def extract_description_bullets(soup):
    bullets = []
    fb = soup.select_one("#feature-bullets")
    if fb:
        for li in fb.select("ul li"):
            text = li.get_text(separator=" ", strip=True)
            if text:
                bullets.append(text)
    if not bullets:
        detail = soup.select_one("#productDetails_feature_div")
        if detail:
            for li in detail.select("li"):
                text = li.get_text(separator=" ", strip=True)
                if text:
                    bullets.append(text)
    if not bullets:
        ul = soup.select_one("ul.a-unordered-list.a-vertical.a-spacing-mini")
        if ul:
            for li in ul.select("li"):
                text = li.get_text(separator=" ", strip=True)
                if text:
                    bullets.append(text)
    return " | ".join(bullets) if bullets else "Not Available"

def extract_barcode(soup):
    keys_to_check = ["EAN","EAN-13","UPC","Barcode","ASIN","Manufacturer barcode"]
    pd = soup.select_one("#productDetails_detailBullets_sections1")
    if pd:
        for row in pd.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if th and td:
                k = th.get_text(strip=True)
                v = td.get_text(strip=True)
                for key in keys_to_check:
                    if key.lower() in k.lower():
                        return v
    text = soup.get_text(separator="\n")
    for key in keys_to_check:
        for line in text.splitlines():
            if key.lower() in line.lower():
                if ":" in line:
                    return line.split(":",1)[1].strip()
                parts = line.split()
                if len(parts) > 1:
                    return parts[-1].strip()
    import re
    cand = re.findall(r"\b\d{8,14}\b", text)
    if cand:
        return cand[0]
    return "Not Available"

# ---------------- MAIN -----------------
def main():
    user_agents = load_user_agents(USER_AGENTS_FILE)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row

    ensure_scraped_column(conn)
    prepare_output_table(conn)

    rows = fetch_all_rows(conn)
    logging.info("Total rows to scrape: %d", len(rows))
    if not rows:
        return

    for r in tqdm(rows, desc="Scraping Products"):
        scrape_id, serial, matched_product_name, url, status = r
        ua = random.choice(user_agents) if user_agents else "Mozilla/5.0"
        headers = BASE_HEADERS.copy()
        headers["User-Agent"] = ua

        now = datetime.utcnow().isoformat()

        # Fill Not Available row if URL missing or status indicates not available
        if not url or "Not Available" in status:
            row_out = (
                scrape_id, serial, "Not Available", matched_product_name,
                "Not Available", "Not Available", "Not Available", "Not Available",
                url if url else "Not Available", SOURCE_WEBSITE, now
            )
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            mark_scraped(conn, scrape_id)
            continue

        resp = get_page(url, headers)
        if resp is None or not hasattr(resp, "status_code") or resp.status_code >= 400:
            logging.warning("Failed to fetch %s", url)
            row_out = (
                scrape_id, serial, "Not Available", matched_product_name,
                "Not Available", "Not Available", "Not Available", "Not Available",
                url, SOURCE_WEBSITE, now
            )
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            mark_scraped(conn, scrape_id)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        page_text = soup.get_text(" ", strip=True).lower()
        if any(x in page_text for x in ["robot check","press and hold","enter the characters you see"]):
            logging.warning("Blocked by anti-bot: %s", url)
            row_out = (
                scrape_id, serial, "Not Available", matched_product_name,
                "Blocked by anti-bot", "Not Available", "Not Available", "Not Available",
                url, SOURCE_WEBSITE, now
            )
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            mark_scraped(conn, scrape_id)
            continue

        product_name = extract_title(soup) or "Not Available"
        description = extract_description_bullets(soup)
        price = extract_price(soup) or "Not Available"
        image_url = extract_image(soup) or "Not Available"
        barcode = extract_barcode(soup) or "Not Available"

        row_out = (
            scrape_id, serial, product_name, matched_product_name,
            description, price, image_url, barcode, url, SOURCE_WEBSITE, now
        )

        append_to_csv(row_out)
        insert_output_table(conn, row_out)
        mark_scraped(conn, scrape_id)

        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    conn.close()
    logging.info("Scraping complete.")

if __name__ == "__main__":
    main()
