import os
import numpy as np
import pandas as pd

from tqdm import tqdm
import json

from sentence_transformers import SentenceTransformer


from chromadb import HttpClient
from chromadb.config import Settings

EMBEDDINGS_NPY_PATH = os.path.join("data", "embeddings", "embeddings.npy")
METADATA_EMBEDDINGS_PATH = os.path.join("data", "metadata_embeddings.csv")

CHROMA_HOST = "localhost"
CHROMA_PORT = 8001
COLLECTION_NAME = "legal_chunks"

BATCH_SIZE = 128

def connect_to_chroma():
    """Initialize Chroma HTTP client."""
    client = HttpClient(
        host=CHROMA_HOST,
        port=CHROMA_PORT,
        settings=Settings(allow_reset=True)
    )
    return client

def load_data():
    embeddings = np.load(EMBEDDINGS_NPY_PATH)
    df = pd.read_csv(METADATA_EMBEDDINGS_PATH, dtype=str)

    return embeddings, df

def build_collection(client):
    """Create or get a Chroma collection."""
    try:
        collection = client.get_collection(name=COLLECTION_NAME)

    except:
        collection = client.create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"}  # distance metric
        )
        print(f"Created collection '{COLLECTION_NAME}'.")

    return collection

def existing_ids(collection):
    """Fetch existing document IDs to avoid duplicates."""

    try:
        data = collection.get(include=["metadatas"])
        return set(data.get("ids", []))
    except Exception:
        return set()


def create_db():

    client = connect_to_chroma()

    embeddings, df = load_data()

    collection = build_collection(client)

    existing = existing_ids(collection)
    # existing = set(["68278f2581cbc78d2015c3fe54d4d337c7d9e5ac39879bfdbc6f24c1fcf2b6a5.html_1"])

    # Select rows not yet in DB
    new_rows_idx = df.index[~df["chunk_id"].isin(existing)].tolist()
    new_rows = df.iloc[new_rows_idx].reset_index(drop=True)

    insert = True
    if not len(new_rows):

        print("Nothing new to insert.")
        insert = False
    

    if insert:
        # Insert batch-by-batch
        for start in tqdm(range(0, len(new_rows), BATCH_SIZE)):
            end = start + BATCH_SIZE

            batch = new_rows.iloc[start:end]
            batch_embeddings = embeddings[ new_rows_idx[start:end] ]

            # batch_embeddings = embeddings[ batch["embedding_index"].values.astype(int)]

            collection.add(

                ids=batch["chunk_id"].tolist(),
                
                embeddings=batch_embeddings.tolist(),
                
                metadatas=batch.apply(lambda r: {
                    "doc_id": r["doc_id"],
                    "doc_processed_path": r["doc_processed_path"],
                    "chunk_id": r["chunk_id"],
                    "chunk_hash": r["chunk_hash"],
                    "timestamp": r["timestamp"]
                }, axis=1).tolist()
            )

        print("Insertion complete!")    

    # Test retrieval
    print("Performing a test query:")
    try:

        model = SentenceTransformer("Amanda/bge_portuguese_v4")
        query_emb = model.encode(" \
Notifique-se. \
Lisboa, 2 de dezembro de 2025 \
O Vice-Presidente do Supremo Tribunal de Justiça \
Nuno Gonçalves")

        res = collection.query(
            query_embeddings=[query_emb.tolist()],
            n_results=3
        )
        print("\n=== Test Results ===")
        print(json.dumps(res, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Query test failed: {e}")


if __name__ == "__main__":
    create_db()