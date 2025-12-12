from typing import List, Dict, Optional
from api.models.model_loader import load_model
from api.db.connection_loader import get_chroma_collection


def retrieve_close_chunks(
        query: str,
        top_k: int = 5,
        filters: Optional[Dict[str, str]] = None
) -> List[Dict]:
    
    # 1. Embed the query with the embedding model
    model = load_model()
    query_embedding = model.encode(query, normalize_embeddings=True).tolist()

    # 2. Access the vector database collection
    collection = get_chroma_collection()

    # 3. Filters
    # chroma_filter = filters if filters else {}

    # 4. Launch the query
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
    )

    ids = results["ids"][0]
    distances = results["distances"][0]
    metadatas = results["metadatas"][0]

    # 5. Collect Results
    ranking = []
    for i in range(len(ids)):
        ranking.append({
            "chunk_id": ids[i],
            "distance": distances[i],
            "metadata": metadatas[i]
        })

    # 6. Print
    print("[Retrieval] Ranking:")
    for r in ranking:
        print(f"  - ID {r['chunk_id']} | score={r['distance']:.4f}")

    return ranking