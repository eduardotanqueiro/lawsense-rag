from typing import List, Dict, Optional
from api.models.emb_loader import load_emb_model
from api.db.connection_loader import get_chroma_collection

from langchain.tools import tool

@tool
def retrieve_close_chunks(
        query: str,
        top_k: int = 5
) -> List[Dict]:
    """Retrieve information to help answer a query."""
    
    # 1. Embed the query with the embedding model
    model = load_emb_model()
    query_embedding = model.encode(query, normalize_embeddings=True).tolist()

    # 2. Access the vector database collection
    collection = get_chroma_collection()

    # 3. Launch the query (get closest chunks and their content)
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
    )

    ids = results["ids"][0]
    distances = results["distances"][0]
    metadatas = results["metadatas"][0]
    documents = results["documents"][0]

    # 4. Collect Results
    ranking = []
    for i in range(len(ids)):
        ranking.append({
            "chunk_id": ids[i],
            "distance": distances[i],
            "metadata": metadatas[i],
            "content": documents[i]
        })

    # 5. Print
    print("[Retrieval] Ranking:")
    for r in ranking:
        print(f"  - ID {r['chunk_id']} | score={r['distance']:.4f} | content='{r['content'][:50]}...'")

    return ranking