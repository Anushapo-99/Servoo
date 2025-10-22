# Cosine Similarity Computation between Input Titles and URL Titles
"""Cosine Similarity Module for Matching Input Titles to URL Titles
This module computes cosine similarity scores between an input product title and up to
five candidate URL titles per record, persists a summary result into a SQLite database,
and writes an augmented JSON output with similarity scores and match decisions.
Configuration constants (modifiable):
- INPUT_JSON:  Path to the input JSON file containing records to process.
- OUTPUT_JSON: Path where the augmented output JSON will be written.
- DB_PATH:     Path to the SQLite database file used to store summary results.
- LOG_FILE:    Path to the log file used by the logging module.
Primary behaviors:
- Loads a list of records from INPUT_JSON. Each record is expected to be a dict with
    at least the following keys:
        - "id"              : Unique identifier for the record (string recommended)
        - "serial_number"   : Optional serial number or secondary identifier
        - "input_title"     : The text title to match against candidate URL titles
        - "url_1", ...       : Candidate URL strings (up to url_5)
        - "url_1_title", ... : Corresponding titles for each candidate URL
    The code gracefully skips missing url_i_title fields.
- For each record, computes TF-IDF vectors for the input_title and each available
    url_i_title, then calculates cosine similarity using scikit-learn utilities
    (TfidfVectorizer and cosine_similarity). Similarity values are rounded to 3
    decimal places when stored in the output record.
- Determines the best match among available candidate URLs by maximum similarity.
    Uses a decision threshold of 0.6 to mark a match as "Available". If no similarity
    reaches this threshold, the status is set to "Not Available" and matched_url is
    set to the string "Not available everywhere".
- Persists a summary row per record into a SQLite table named url_similarity_results
    with the following columns (created if not present):
        - id (TEXT)
        - serial_number (TEXT)
        - input_title (TEXT)
        - matched_url (TEXT)
        - status (TEXT)
- Writes an output JSON file containing the original record augmented with:
        - "url_i_similarity" keys for computed similarities (for each url_i_title present)
        - "best_similarity" (float, rounded to 3 decimals)
        - "matched_url" (URL string or "Not available everywhere")
        - "status" ("Available" or "Not Available")
Logging and error handling:
- Uses Python's logging module to write INFO/WARNING/ERROR messages to LOG_FILE.
- Logs progress such as number of records loaded, per-record processing details,
    similarity values for each candidate, and whether a match was found for each record.
- Any uncaught exception during processing is logged with stack trace and printed to stdout.
Dependencies:
- scikit-learn (for TfidfVectorizer and cosine_similarity)
- Python standard library: json, sqlite3, logging
Usage:
- Run the module as a script. It will initialize the database table if necessary,
    process all records in INPUT_JSON, write augmented results to OUTPUT_JSON, and
    insert summary rows into the SQLite database at DB_PATH. Log messages are written
    to LOG_FILE.
Notes and considerations:
- The TF-IDF vectorizer is re-fit for each pairwise comparison (input_title vs candidate).
    For large datasets or many comparisons this could be optimized by fitting on a larger
    corpus and reusing the vectorizer.
- Similarity threshold (0.6) can be adjusted depending on desired matching strictness.
- All text comparisons are lowercased before computing similarity to reduce case-sensitivity.
- Input validation is minimal; ensure JSON records conform to the expected structure.
"""

# importing necessary libraries
import json
import sqlite3
import logging
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

#CONFIG
INPUT_JSON = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/DATA/top_product_urls.json"
OUTPUT_JSON = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/DATA/top_product_urls_with_similarity.json"
DB_PATH = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/DATA/Url_output_amazon.db"
LOG_FILE = "/home/anusha/Desktop/Servoo/DATA_SCRAPING/LOG/cosine_similarity_log.txt"

#LOGGING
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

#DATABASE SETUP
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS url_similarity_results (
            id TEXT,
            serial_number TEXT,
            input_title TEXT,
            matched_url TEXT,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()



#COSINE SIMILARITY FUNCTION
def compute_similarity(text1, text2):
    vectorizer = TfidfVectorizer().fit([text1, text2])
    vectors = vectorizer.transform([text1, text2])
    return float(cosine_similarity(vectors[0], vectors[1])[0][0])


#MAIN PROCESS
def main():
    try:
        init_db()
        logging.info("Database initialized successfully.")
        
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)

        logging.info(f"Loaded {len(data)} records from input JSON.")

        output_data = []
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        for record in data:
            record_id = record.get("id")
            serial_number = record.get("serial_number")
            input_title = record.get("input_title", "").strip()

            logging.info(f"Processing ID={record_id}, Title='{input_title}'")

            similarities = {}
            best_similarity = 0
            best_url = None

            # Compute similarity for each URL title
            for i in range(1, 6):
                url_title_key = f"url_{i}_title"
                url_key = f"url_{i}"

                if url_title_key not in record or not record[url_title_key]:
                    continue

                similarity = compute_similarity(input_title.lower(), record[url_title_key].lower())
                similarities[f"url_{i}_similarity"] = round(similarity, 3)

                if similarity > best_similarity:
                    best_similarity = similarity
                    best_url = record[url_key]

                logging.info(f"→ url_{i}: Similarity = {similarity:.3f}")

            # Add similarity scores to record
            record.update(similarities)

            # Save result to database
            if best_similarity >= 0.6:
                status = "Available"
                matched_url = best_url
                logging.info(f"✅ Match found (similarity={best_similarity:.3f}) for ID={record_id}")
            else:
                status = "Not Available"
                matched_url = "Not available everywhere"
                logging.warning(f"❌ No good match found for ID={record_id} (max similarity={best_similarity:.3f})")

            cur.execute(
                "INSERT INTO url_similarity_results (id, serial_number, input_title, matched_url, status) VALUES (?, ?, ?, ?, ?)",
                (record_id, serial_number, input_title, matched_url, status)
            )

            # Update record
            record["best_similarity"] = round(best_similarity, 3)
            record["matched_url"] = matched_url
            record["status"] = status
            output_data.append(record)

        conn.commit()
        conn.close()
        logging.info("All results saved to database successfully.")

        # Save output JSON
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Output saved to JSON file: {OUTPUT_JSON}")

        print("✅ Cosine similarity computation completed successfully.")
        print(f"→ Results saved to: {OUTPUT_JSON}")
        print(f"→ Database updated at: {DB_PATH}")
        print(f"→ Log file: {LOG_FILE}")

    except Exception as e:
        logging.error(f"Error occurred: {e}", exc_info=True)
        print(f"❌ Error occurred: {e}")


if __name__ == "__main__":
    main()
