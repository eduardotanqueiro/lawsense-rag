import os
import csv
import re
import fitz  # PyMuPDF
from bs4 import BeautifulSoup

import hashlib
import csv
from datetime import datetime, timezone

METADATA_RAW_PATH = os.path.join("data", "metadata_raw.csv")
PROCESSED_BASE = os.path.join("data", "processed")
METADATA_PROCESSED_PATH = os.path.join("data", "metadata_processed.csv")

def save_metadata(row):
    file_exists = os.path.exists(METADATA_PROCESSED_PATH)

    with open(METADATA_PROCESSED_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "source_path", "target_path", "timestamp", "source_hash", "target_hash"]
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# Helper: cleaning and normalization
def clean_text(text: str) -> str:
    # Normalize whitespace
    text = text.replace("\r", "")
    text = re.sub(r"\n{2,}", "\n\n", text)

    # Remove common page numbers
    text = re.sub(r"^p[aá]gina\s*\d+\s*$", "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"^\s*\d+\s*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^—?\s*\d+\s*—?$", "", text, flags=re.MULTILINE)

    # Remove extra empty lines
    text = re.sub(r"\n\s*\n+", "\n\n", text)

    return text.strip()

    # return re.sub(r'\s+', ' ', text).strip()

# PDF Extraction
def extract_pdf(path: str) -> str:
    doc = fitz.open(path)
    texts = []
    for page in doc:
        texts.append(page.get_text("text"))
    return "\n".join(texts)


# HTML Extraction
def extract_html(path: str) -> str:
    with open(path, "rb") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    # Remove scripts, navigation, styles
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    return text


# TXT Extraction
def extract_txt(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


# Dispatcher
def extract_file(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()

    if ext == ".pdf":
        return extract_pdf(path)
    elif ext in [".html", ".htm"]:
        return extract_html(path)
    elif ext in [".txt"]:
        return extract_txt(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")



def run_extraction():
    os.makedirs(PROCESSED_BASE, exist_ok=True)

    with open(METADATA_RAW_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            raw_path = row["file_path"]
            file_id = row["id"]
            source_hash = row["hash"]
            output_path = os.path.join(PROCESSED_BASE, file_id + ".txt")

            if os.path.exists(output_path):
                # TODO: check hash to see if re-extraction is needed
                continue  # skip already processed

            try:
                text = extract_file(raw_path)
                text = clean_text(text)

                with open(output_path, "w", encoding="utf-8") as out:
                    out.write(text)

                # ["id", "source_path", "target_path", "timestamp", "source_hash", "target_hash"]
                save_metadata({
                    "id": file_id,
                    "source_path": raw_path,
                    "target_path": output_path,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source_hash": source_hash,
                    "target_hash": hashlib.sha256(text.encode("utf-8")).hexdigest()
                })

                print(f"[ETL] Processed: {output_path}")

            except Exception as e:
                print(f"[ETL] Failed to process {raw_path}: {e}")


if __name__ == "__main__":
    run_extraction()
