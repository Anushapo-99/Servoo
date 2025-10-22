import json
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ---------------- CONFIG ---------------- #
INPUT_JSON = "/home/anusha/Desktop/top_product_urls.json"
OUTPUT_JSON = "/home/anusha/Desktop/top_product_urls_with_similarity.json"
DB_PATH = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/DATA/Url_output_amazon.db"
LOG_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_SCRAPING_TASK/Logs/cosine_similarity.log"

# ---------------- LOGGING SETUP ---------------- #
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.info("=== Cosine Similarity Script Started ===")

# ---------------- DATABASE SETUP ---------------- #
def setup_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS url_similarity (
            id TEXT,
            serial_number TEXT,
            input_title TEXT,
            url_1 TEXT, url_1_title TEXT, url_1_similarity REAL,
            url_2 TEXT, url_2_title TEXT, url_2_similarity REAL,
            url_3 TEXT, url_3_title TEXT, url_3_similarity REAL,
            url_4 TEXT, url_4_title TEXT, url_4_similarity REAL,
            url_5 TEXT, url_5_title TEXT, url_5_similarity REAL,
            scraped_date TEXT,
            processed_date TEXT
        )
    """)
    conn.commit()
    return conn

# ---------------- COSINE SIMILARITY ---------------- #
def compute_similarity(text1, text2):
    """Compute cosine similarity between two strings."""
    try:
        vectorizer = TfidfVectorizer(stop_words="english")
        tfidf = vectorizer.fit_transform([text1, text2])
        sim = cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0]
        return round(float(sim), 3)
    except Exception as e:
        logging.error(f"Error computing similarity: {e}")
        return 0.0

# ---------------- MAIN PROCESS ---------------- #
def process_json():
    logging.info(f"Loading input JSON: {INPUT_JSON}")
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = setup_database()
    cursor = conn.cursor()
    output_data = []

    for record in data:
        try:
            input_title = record.get("input_title", "")
            record_id = record.get("id", "")
            serial_number = record.get("serial_number", "")
            logging.info(f"Processing ID={record_id}, Serial={serial_number}, Title='{input_title}'")

            # Compute similarities for up to 5 URLs
            for i in range(1, 6):
                url_title_key = f"url_{i}_title"
                sim_key = f"url_{i}_similarity"
                title_text = record.get(url_title_key, "")

                if title_text:
                    sim_score = compute_similarity(input_title, title_text)
                    record[sim_key] = sim_score
                    logging.debug(f"Similarity for {url_title_key}: {sim_score}")
                else:
                    record[sim_key] = None
                    logging.warning(f"No title found for {url_title_key}")

            record["processed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            output_data.append(record)

            # Insert into database
            # cursor.execute("""
            #     INSERT INTO url_similarity (
            #         id, serial_number, input_title,
            #         url_1, url_1_title, url_1_similarity,
            #         url_2, url_2_title, url_2_similarity,
            #         url_3, url_3_title, url_3_similarity,
            #         url_4, url_4_title, url_4_similarity,
            #         url_5, url_5_title, url_5_similarity,
            #         scraped_date, processed_date
            #     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            # """, (
            #     record.get("id"),
            #     record.get("serial_number"),
            #     input_title,
            #     record.get("url_1"), record.get("url_1_title"), record.get("url_1_similarity"),
            #     record.get("url_2"), record.get("url_2_title"), record.get("url_2_similarity"),
            #     record.get("url_3"), record.get("url_3_title"), record.get("url_3_similarity"),
            #     record.get("url_4"), record.get("url_4_title"), record.get("url_4_similarity"),
            #     record.get("url_5"), record.get("url_5_title"), record.get("url_5_similarity"),
            #     record.get("scraped_date"),
            #     record["processed_date"]
            # ))
            cursor.execute("""
            INSERT INTO url_similarity (
                id, serial_number, input_title,
                url_1, url_1_title, url_1_similarity,
                url_2, url_2_title, url_2_similarity,
                url_3, url_3_title, url_3_similarity,
                url_4, url_4_title, url_4_similarity,
                url_5, url_5_title, url_5_similarity,
                scraped_date, processed_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.get("id"),
            record.get("serial_number"),
            input_title,
            record.get("url_1"), record.get("url_1_title"), record.get("url_1_similarity"),
            record.get("url_2"), record.get("url_2_title"), record.get("url_2_similarity"),
            record.get("url_3"), record.get("url_3_title"), record.get("url_3_similarity"),
            record.get("url_4"), record.get("url_4_title"), record.get("url_4_similarity"),
            record.get("url_5"), record.get("url_5_title"), record.get("url_5_similarity"),
            record.get("scraped_date"),
            record["processed_date"]
        ))

            conn.commit()

        except Exception as e:
            logging.error(f"Error processing record ID={record.get('id')}: {e}")

    conn.close()

    # Write updated JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f_out:
        json.dump(output_data, f_out, indent=4, ensure_ascii=False)

    logging.info(f"Results saved to JSON: {OUTPUT_JSON}")
    logging.info(f"Results saved to SQLite DB: {DB_PATH}")
    logging.info("=== Cosine Similarity Script Completed ===")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
    process_json()
