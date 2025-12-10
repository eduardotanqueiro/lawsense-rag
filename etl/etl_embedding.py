import os
import csv
import json
import pandas as pd
import numpy as np
import argparse

from sentence_transformers import SentenceTransformer
import tqdm


EMBEDDINGS_NPY_PATH = os.path.join("data", "embeddings", "embeddings.npy")
METADATA_EMBEDDINGS_PATH = os.path.join("data", "metadata_embeddings.csv")

CHUNKS_JSONL_PATH = os.path.join("data", "chunked", "chunks.jsonl")
CHUNKS_CSV_PATH = os.path.join("data", "chunked", "chunks.csv")
METADATA_CHUNKED_PATH = os.path.join("data", "metadata_chunked.csv")

os.makedirs(os.path.dirname(EMBEDDINGS_NPY_PATH), exist_ok=True)


# I/O
def read_chunks():

    if os.path.exists(CHUNKS_JSONL_PATH):
        print(f"Loading chunks from {CHUNKS_JSONL_PATH}")
        chunks = []

        with open(CHUNKS_JSONL_PATH, "r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                chunks.append(json.loads(line))

        return chunks

    if os.path.exists(CHUNKS_CSV_PATH):
        print(f"Loading chunks from {CHUNKS_CSV_PATH}")

        df = pd.read_csv(CHUNKS_CSV_PATH, dtype=str)
        
        # Ensure proper column types and content
        rows = df.to_dict(orient="records")
        for r in rows:
            # keep content as-is; ensure tokens and indices numeric if present
            r["tokens"] = int(r.get("tokens") or 0)
            r["chunk_index"] = int(r.get("chunk_index") or 0)

        return rows

    raise FileNotFoundError(f"No chunks found at {CHUNKS_JSONL_PATH} or {CHUNKS_CSV_PATH}.")

def save_data(embeddings, metadata):

    # Save embeddings
    if os.path.exists(EMBEDDINGS_NPY_PATH):
        print("Existing embeddings found. Loading for append")

        # Append to existing embeddings
        existing_emb = np.load(EMBEDDINGS_NPY_PATH)
        all_emb = np.vstack([existing_emb, embeddings])

        np.save(EMBEDDINGS_NPY_PATH, all_emb)
    else:

        print("No existing embeddings found. Creating new file.")
        np.save(EMBEDDINGS_NPY_PATH, embeddings)

    # Save metadata
    file_exists = os.path.exists(METADATA_EMBEDDINGS_PATH)

    with open(METADATA_EMBEDDINGS_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=metadata[0].keys())
        
        if not file_exists:
            writer.writeheader()

        writer.writerows(metadata)

# 
def generate_embeddings(model, chunks: list, batch_size: int, device: str):
    emb = model.encode(chunks, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=False)
    return emb


#
def create_embeddings(model_name: str, batch_size: int, device: str):

    model = SentenceTransformer(model_name, device=device)
    print(f"Model loaded. {model_name} on device {model.device}...")

    # Read embeddings metadata
    current_emb_hashes = {}

    if os.path.exists(METADATA_EMBEDDINGS_PATH):
        with open(METADATA_EMBEDDINGS_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                # ERROR HERE
                doc_path = row["doc_processed_path"]
                chunk_hash = row["chunk_hash"]

                current_emb_hashes[chunk_hash] = doc_path


    # Read chunks
    chunks = read_chunks()

    # Read chunk metadata
    chunks_hashs = {}
    with open(METADATA_CHUNKED_PATH, "r",newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            chunk_id = row["chunk_id"]
            chunk_hash = row["hash"]
            chunk_proc_path = row["doc_processed_path"]

            chunks_hashs[chunk_id] = [chunk_hash, chunk_proc_path]


    # Process each chunk
    to_embed = []
    to_embed_metadata = []
    num_to_embed = 0
    batch_num = 0

    for curr_chunk in chunks:
        curr_chunk_id = curr_chunk["chunk_id"]

        curr_chunk_hash = chunks_hashs[curr_chunk_id][0]
        curr_chunk_proc_path = chunks_hashs[curr_chunk_id][1]


        # Check if chunk is already embedded (metadata)
        if curr_chunk_hash in current_emb_hashes:
            print(f"[EMBEDDING] Skipping {curr_chunk_id} with hash {curr_chunk_hash}, already embedded.")
            continue

        
        to_embed.append(curr_chunk["content"])
        to_embed_metadata.append({
            "doc_id": curr_chunk["doc_id"],
            "doc_processed_path": curr_chunk_proc_path,
            "chunk_id": curr_chunk_id,
            "chunk_hash": curr_chunk_hash,
            "timestamp": pd.Timestamp.now().isoformat()
        })
        num_to_embed += 1
        
        if num_to_embed % batch_size == 0 or num_to_embed == len(chunks):
            print(f"[EMBEDDING] Processing batch num {batch_num} of {num_to_embed} chunks...")
            batch_num += 1

            # Create embeddings from chunks
            curr_embeddings = generate_embeddings(model, to_embed, batch_size, device)

            # Save embedding and metadata
            save_data(curr_embeddings, to_embed_metadata)

            # Clear variables to free memory
            to_embed = []
            to_embed_metadata = []

    




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Embed chunked documents with SentenceTransformers")
    
    parser.add_argument("--model-name", type=str, default="all-MiniLM-L6-v2", help="SentenceTransformer model name")
    parser.add_argument("--batch-size", type=int, default=64, help="Batch size for encoding")
    parser.add_argument("--device", type=str, default="cpu", help="Device to run model on (e.g., cpu, cuda:0)")
    
    args = parser.parse_args()

    create_embeddings(args.model_name, args.batch_size, args.device)