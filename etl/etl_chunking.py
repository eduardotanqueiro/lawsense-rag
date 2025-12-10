import os
from pathlib import Path
import pandas as pd
import csv
import json

import hashlib
import tiktoken

PROCESSED_BASE = os.path.join("data", "processed")
METADATA_PROCESSED_PATH = os.path.join("data", "metadata_processed.csv")
OUTPUT_CHUNK_PATH = os.path.join("data", "chunked")
METADATA_CHUNKED_PATH = os.path.join("data")

ENCODER = tiktoken.get_encoding("cl100k_base")  # OpenAI tokenizer


def split_into_paragraphs(text: str):
    # split on blank lines
    return [p.strip() for p in text.split("\n\n") if p.strip()]

def count_tokens(text: str) -> int:
    return len(ENCODER.encode(text))


def chunk_paragraphs(paragraphs, max_tokens=500):
    chunks = []
    current = []
    current_tokens = 0

    for para in paragraphs:
        paragraph_tokens = count_tokens(para)

        # If a single paragraph is too big, split by sentences instead
        if paragraph_tokens > max_tokens:
            sentences = [s.strip() for s in para.split(". ") if s.strip()]

            for sent in sentences:
                sent_tokens = count_tokens(sent)
            
                if current_tokens + sent_tokens > max_tokens:
                    chunks.append("\n".join(current))
                    current = [sent]
                    current_tokens = sent_tokens
                else:
                    current.append(sent)
                    current_tokens += sent_tokens
            continue

        # Normal paragraph append
        if current_tokens + paragraph_tokens > max_tokens:
            chunks.append("\n\n".join(current))
            current = [para]
            current_tokens = paragraph_tokens
        else:
            current.append(para)
            current_tokens += paragraph_tokens

    if current:
        chunks.append("\n\n".join(current))

    return chunks


def process_clean_file(metadata_row, output_rows, metadata_chunked_rows):

    path = Path(metadata_row["target_path"])


    text = path.read_text(encoding="utf-8")
    paragraphs = split_into_paragraphs(text)
    chunks = chunk_paragraphs(paragraphs)

    for idx, chunk_text in enumerate(chunks):
        token_count = count_tokens(chunk_text)

        output_rows.append({
            "doc_id": metadata_row["id"],
            "chunk_id": f"{metadata_row["id"]}_{idx}",
            "chunk_index": idx,
            "tokens": token_count,
            "content": chunk_text
        })

        metadata_chunked_rows.append({
            "doc_id": metadata_row["id"],
            "chunk_id": f"{metadata_row["id"]}_{idx}",
            "chunk_index": idx,
            "timestamp": pd.Timestamp.now().isoformat(),
            "doc_processed_path": f"{metadata_row["target_path"]}",
            "hash": hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
        })




def run_dispatcher():


    # Check for necessary files and directories
    if not os.path.exists(METADATA_PROCESSED_PATH):
        print("[ETL] No processed metadata found. Skipping chunking.")
        return -1
    
    if not os.path.exists(PROCESSED_BASE):
        print("[ETL] No processed files found. Skipping chunking.")
        return -1
    
    os.makedirs(OUTPUT_CHUNK_PATH, exist_ok=True)

    metadata_processed_rows = pd.read_csv(METADATA_PROCESSED_PATH)

    output_rows = []
    metadata_chunked_rows = [] if os.path.exists( os.path.join(METADATA_CHUNKED_PATH, "metadata_chunked.csv") ) == False else pd.read_csv(os.path.join(METADATA_CHUNKED_PATH, "metadata_chunked.csv")).to_dict(orient="records")

    # Process each row of the metadata (each row corresponds to a processed file)
    for _, row in metadata_processed_rows.iterrows():

        if any(chunk["doc_id"] == row["id"] for chunk in metadata_chunked_rows):
            # Already chunked
            print(f"[CHUNK] Skipping {row["id"]}, already chunked.")
            continue

        print(f"[CHUNK] Processing {row["id"]}")
        process_clean_file(row, output_rows, metadata_chunked_rows)

    # Output paths
    output_metadata = os.path.join(METADATA_CHUNKED_PATH, "metadata_chunked.csv")
    output_csv = os.path.join(OUTPUT_CHUNK_PATH, "chunks.csv")
    output_jsonl = os.path.join(OUTPUT_CHUNK_PATH, "chunks.jsonl")

    # Write CSV
    with open(output_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["doc_id", "chunk_id", "chunk_index", "tokens", "content"])
        writer.writeheader()
        writer.writerows(output_rows)

    # Write JSONL
    with open(output_jsonl, "a", encoding="utf-8") as f:
        for row in output_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


    # Write metadata
    with open(output_metadata, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["doc_id", "chunk_id", "chunk_index", "timestamp", "doc_processed_path", "hash"])
        writer.writeheader()
        writer.writerows(metadata_chunked_rows)


    print(f"[CHUNK] Chunking completed. Output saved to {OUTPUT_CHUNK_PATH}.\n[CHUNK] Number of chunks created: {len(output_rows)}")


if __name__ == "__main__":
    run_dispatcher()