import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from vectorstore.embedings import embed_text, embed_batch
from vectorstore.client import QdrantClient

from sentence_transformers import CrossEncoder

reranker = CrossEncoder("BAAI/bge-reranker-large")

qdrant_url = "https://3263bd08-1ccd-4714-aead-51d975df75e1.us-west-2-0.aws.cloud.qdrant.io:6333"
qdrant_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwiZXhwIjoxNzY5Nzk0MDU4fQ.PdqsxAYyGWriMiyQMBUOEjqzW4n3L_AyfNYQdOcmFpc"


async def hybrid_rerank_search(query, top_k=30):
    q = QdrantClient(qdrant_url=qdrant_url, qdrant_api_key=qdrant_api_key)
    vec = await embed_batch(query)
    if isinstance(vec[0], list): 
        vec = vec[0]
    results = await q.hybrid_query(vec, limit=top_k)#type: ignore
    
    scored = [(r["content"], r["score"]) for r in results]

    rerank_inputs = [(query, doc) for doc, _ in scored]
    rerank_scores = reranker.predict(rerank_inputs)
    ranked = sorted(zip(results, rerank_scores), key=lambda x: x[1], reverse=True)
    return [r[0] for r in ranked[:10]]


async def search(query, limit=20):
    res = await hybrid_rerank_search(query, top_k=limit)
    return res


from pydantic import BaseModel


class AdvanceTariff(BaseModel):
    query :str 



async def addtional_tariff_check(query: str):
    """
    Searches the tariff tracker for additional or reciprocal tariffs.
    Returns the raw results enriched with all textual metadata,
    excluding only invalid or non-textual fields (e.g. image_url, file_id).
    """
    results = await search(query)
    cleaned_results = []

    for r in results:
        meta = r.get("meta", {}) or {}

        safe_meta = {
            k: v
            for k, v in meta.items()
            if k not in ("image_url", "file_id", "thumbnail", "embedding", "vector","doc_source")
        }

        cleaned_results.append({
            "id": r.get("id"),
            "score": float(r.get("score", 0.0)),
            "content": r.get("content"),
            "meta": safe_meta,
            "page": r.get("page"),
            "checksum": r.get("checksum"),
            "is_table": r.get("is_table", False)
        })

    return cleaned_results




if __name__ == "__main__":
    import asyncio
    print(asyncio.run(addtional_tariff_check("which tariffs will apply on products imported from china")))