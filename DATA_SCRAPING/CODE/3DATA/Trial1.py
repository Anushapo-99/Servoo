#!/usr/bin/env python3
"""
amazon_ae_scraper.py

Usage: python amazon_ae_scraper.py

Requirements:
- curl-cffi
- beautifulsoup4
- lxml
- pandas
- tqdm

This script:
- Reads rows from matched_products in the DB_FILE
- Scrapes matched_url using curl-cffi with rotating user-agents
- Writes results to Servoo_Scraped_Data.csv and SQLite table scraped_products
- Marks matched_products.scraped = 1 after each processed row
"""

import sqlite3
import time
import random
import csv
import os
import logging
from datetime import datetime
from tqdm import tqdm

from curl_cffi import requests   # curl-cffi provides a requests-like API
from bs4 import BeautifulSoup
import pandas as pd

# ------------------ USER CONFIG ------------------
DB_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/DATA/Url_output_amazon.db"
USER_AGENTS_FILE = "/home/anusha/Desktop/DATAHUT/Macys_clothing/user_agents.txt"
OUTPUT_CSV = "Servoo_Scraped_Data.csv"
OUTPUT_TABLE = "scraped_products"

# Default headers base (we'll replace User-Agent each request)
BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

# Random delay config (seconds)
MIN_DELAY = 2.0
MAX_DELAY = 6.0

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
# --------------------------------------------------

def load_user_agents(path):
    if not os.path.exists(path):
        logging.error("User agents file not found: %s", path)
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [l.strip() for l in f if l.strip()]
    logging.info("Loaded %d user agents", len(lines))
    return lines

def ensure_scraped_column(conn):
    cur = conn.cursor()
    # Check if column exists
    cur.execute("PRAGMA table_info(matched_products)")
    cols = [r[1] for r in cur.fetchall()]
    if "scraped" not in cols:
        logging.info("Adding 'scraped' column to matched_products table")
        cur.execute("ALTER TABLE matched_products ADD COLUMN scraped INTEGER DEFAULT 0")
        conn.commit()
    else:
        logging.info("'scraped' column already present")

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
            Source_Website TEXT,
            Last_Updated TEXT
        )
    """)
    conn.commit()

def append_to_csv(row, csv_path=OUTPUT_CSV):
    header = ["Scrape_ID","Serial_Number","Product_Name","Matched_Product_Name","Description","Price (AED)","Image_URL","Barcode","Source_Website","Last_Updated"]
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
            Description, Price_AED, Image_URL, Barcode, Source_Website, Last_Updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)
    conn.commit()

def fetch_rows_to_scrape(conn):
    cur = conn.cursor()
    # Select rows with status 'Available' and scraped = 0 (or missing)
    # Some entries might have status values with case differences; adjust as needed
    cur.execute("""
        SELECT id, serial_number, input_title, matched_url, status
        FROM matched_products
        WHERE (status = 'Available' OR status = 'available') AND (IFNULL(scraped,0) = 0)
    """)
    return cur.fetchall()

def mark_scraped(conn, scrape_id):
    cur = conn.cursor()
    cur.execute("UPDATE matched_products SET scraped = 1 WHERE id = ?", (scrape_id,))
    conn.commit()

def mark_not_available(conn, scrape_id):
    cur = conn.cursor()
    cur.execute("UPDATE matched_products SET status = 'Not Available', scraped = 1 WHERE id = ?", (scrape_id,))
    conn.commit()

# ---------- HTML parsing helpers ----------
def get_page(url, headers, timeout=30):
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        return resp
    except Exception as e:
        logging.exception("HTTP error for %s: %s", url, e)
        return None

def extract_title(soup):
    # Amazon main title span id productTitle or productTitle in sample
    t = soup.select_one("#productTitle")
    if t:
        return " ".join(t.get_text(strip=True).split())
    # fallback: h1.title or titleSection
    h = soup.select_one("h1")
    return " ".join(h.get_text(strip=True).split()) if h else ""

def extract_price(soup):
    # prices on Amazon have several selectors
    selectors = [
        "span#priceblock_ourprice", "span#priceblock_dealprice",
        "span.a-price span.a-offscreen", "span.a-offscreen"
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(strip=True)
    return ""

def extract_image(soup):
    # landingImage id is common
    img = soup.select_one("#imgTagWrapperId img#landingImage")
    if img and img.get("data-old-hires"):
        return img.get("data-old-hires")
    if img and img.get("src"):
        return img.get("src")
    # fallback: meta property og:image
    meta = soup.find("meta", {"property": "og:image"})
    if meta and meta.get("content"):
        return meta.get("content")
    return ""

def extract_description_bullets(soup):
    # bullet lists in feature-bullets or ul.a-unordered-list under #feature-bullets
    bullets = []
    # 1) feature bullets
    fb = soup.select_one("#feature-bullets")
    if fb:
        for li in fb.select("ul li"):
            text = li.get_text(separator=" ", strip=True)
            if text:
                bullets.append(text)
    # 2) productDetails or detailBullets_feature_div
    if not bullets:
        detail = soup.select_one("#productDetails_feature_div")
        if detail:
            for li in detail.select("li"):
                text = li.get_text(separator=" ", strip=True)
                if text:
                    bullets.append(text)
    # 3) general list fallback
    if not bullets:
        ul = soup.select_one("ul.a-unordered-list.a-vertical.a-spacing-mini")
        if ul:
            for li in ul.select("li"):
                text = li.get_text(separator=" ", strip=True)
                if text:
                    bullets.append(text)
    return " | ".join(bullets)

def extract_barcode(soup):
    # try multiple locations where EAN/UPC/ASIN can be present
    # 1) product details table (productDetails_techSpec_section)
    keys_to_check = ["EAN", "EAN-13", "UPC", "Barcode", "ASIN", "Manufacturer barcode"]
    # new Amazon layout: productDetails_detailBullets_sections1
    pd = soup.select_one("#productDetails_detailBullets_sections1")
    if pd:
        # parse table rows
        for row in pd.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if th and td:
                k = th.get_text(strip=True)
                v = td.get_text(strip=True)
                for key in keys_to_check:
                    if key.lower() in k.lower():
                        return v
    # try detail bullets
    # sometimes displayed like "ASIN : B09XXXX"
    text = soup.get_text(separator="\n")
    for key in keys_to_check:
        for line in text.splitlines():
            if key.lower() in line.lower():
                # take last token or after colon
                if ":" in line:
                    return line.split(":",1)[1].strip()
                parts = line.split()
                if len(parts) > 1:
                    return parts[-1].strip()
    # fallback: try to extract 8-14 digit numeric sequences (possible EAN/UPC)
    import re
    cand = re.findall(r"\b\d{8,14}\b", soup.get_text())
    if cand:
        return cand[0]
    return ""

# ---------------- Main workflow -----------------
def main():
    user_agents = load_user_agents(USER_AGENTS_FILE)
    if not user_agents:
        logging.warning("No user agents loaded - proceeding with default UA in BASE_HEADERS")

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row

    ensure_scraped_column(conn)
    prepare_output_table(conn)

    rows = fetch_rows_to_scrape(conn)
    logging.info("Found %d rows to scrape", len(rows))
    if not rows:
        logging.info("Nothing to do. Exiting.")
        return

    for r in tqdm(rows, desc="Products"):
        scrape_id = r[0]
        serial = r[1]
        matched_product_name = r[2]
        url = r[3]
        status = r[4]

        logging.info("Processing id=%s serial=%s url=%s", scrape_id, serial, url)

        # choose UA
        ua = random.choice(user_agents) if user_agents else BASE_HEADERS.get("User-Agent", "Mozilla/5.0")
        headers = BASE_HEADERS.copy()
        headers["User-Agent"] = ua

        # perform request
        resp = get_page(url, headers)
        if resp is None or not hasattr(resp, "status_code"):
            logging.error("No response or bad response for id=%s. Marking Not Available", scrape_id)
            mark_not_available(conn, scrape_id)
            # still record a not available row
            now = datetime.utcnow().isoformat()
            row_out = (
                scrape_id, serial, "", matched_product_name, "Not Available", "", "", url, now
            )
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            continue

        status_code = resp.status_code
        html = resp.text if hasattr(resp, "text") else (resp.content.decode("utf-8", errors="ignore") if hasattr(resp, "content") else "")
        if status_code >= 400 or not html.strip():
            logging.warning("HTTP %s for %s. Marking Not Available", status_code, url)
            mark_not_available(conn, scrape_id)
            now = datetime.utcnow().isoformat()
            row_out = (
                scrape_id, serial, "", matched_product_name, "Not Available", "", "", url, now
            )
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            continue

        soup = BeautifulSoup(html, "lxml")

        # Quick heuristic: if page contains "Robot Check" or "To discuss automated access" etc, treat as blocked
        page_text = soup.get_text(" ", strip=True).lower()
        if "robot check" in page_text or "press and hold" in page_text or "enter the characters you see below" in page_text:
            logging.warning("Blocked by anti-bot for %s. Skipping/marking Not Available", url)
            # Do not aggressively retry here; mark not available
            mark_not_available(conn, scrape_id)
            now = datetime.utcnow().isoformat()
            row_out = (
                scrape_id, serial, "", matched_product_name, "Blocked by anti-bot (robot check)", "", "", url, now
            )
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            continue

        product_name = extract_title(soup) or ""
        description = extract_description_bullets(soup) or ""
        price = extract_price(soup) or ""
        image_url = extract_image(soup) or ""
        barcode = extract_barcode(soup) or ""

        # If we couldn't find title & description & price, consider Not Available
        if not product_name and not description and not price:
            logging.info("No product data found for id=%s. Marking Not Available", scrape_id)
            mark_not_available(conn, scrape_id)
            now = datetime.utcnow().isoformat()
            row_out = (
                scrape_id, serial, "", matched_product_name, "Not Available", "", "", url, now
            )
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            continue

        # Normalize price to include AED if needed (user asked AED preferred)
        # Price value often like 'AED8.95' or 'AED 8.95' or '8.95'
        price_norm = price.replace("\xa0", " ").strip()

        now = datetime.utcnow().isoformat()
        row_out = (
            scrape_id,
            serial,
            product_name,
            matched_product_name,
            description,
            price_norm,
            image_url,
            barcode,
            url,
            now
        )

        try:
            append_to_csv(row_out)
            insert_output_table(conn, row_out)
            mark_scraped(conn, scrape_id)
            logging.info("Saved product id=%s", scrape_id)
        except Exception as e:
            logging.exception("Failed to save product id=%s: %s", scrape_id, e)

        # polite randomized delay
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logging.debug("Sleeping %.2fs", delay)
        time.sleep(delay)

    conn.close()
    logging.info("Done scraping all products.")

if __name__ == "__main__":
    main()
