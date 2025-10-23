import os
from typing import List, Dict, Tuple
import re
import hashlib
from openai import OpenAI
from dotenv import load_dotenv
from sentence_transformers import CrossEncoder
from qdrant_client.http.models import SparseVector

load_dotenv()

def _get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")
    return OpenAI(api_key=api_key)

_openai_client = None


def create_dense_embedding(text: str) -> List[float]:
    global _openai_client
    if _openai_client is None:
        _openai_client = _get_openai_client()
    resp = _openai_client.embeddings.create(model="text-embedding-ada-002", input=text)
    return resp.data[0].embedding



_sparse_embedder = None

def create_sparse_embedding(text: str) -> SparseVector:
    global _sparse_embedder
    try:
        if _sparse_embedder is None:
            # Lazy import to avoid hard dependency on onnxruntime in some environments
            try:
                from fastembed import SparseTextEmbedding  # type: ignore
            except Exception:
                # Fallback to a simple hashed sparse embedding
                tokens = re.findall(r"[a-zA-Z0-9]+", text.lower())
                if not tokens:
                    return SparseVector(indices=[], values=[])
                dim = int(os.environ.get("SPARSE_HASH_DIM", "100000"))
                counts: Dict[int, float] = {}
                for t in tokens:
                    hv = int.from_bytes(hashlib.blake2b(t.encode("utf-8"), digest_size=8).digest(), "big") % dim
                    counts[hv] = counts.get(hv, 0.0) + 1.0
                indices = list(counts.keys())
                values = list(counts.values())
                return SparseVector(indices=indices, values=values)
            _sparse_embedder = SparseTextEmbedding("Qdrant/bm25")
        encoded = list(_sparse_embedder.embed([text]))
        indices = [int(i) for i in encoded[0].indices]
        values = [float(v) for v in encoded[0].values]
        return SparseVector(indices=indices, values=values)
    except Exception:
        # Final guard: hashed fallback if something goes wrong during fastembed call
        tokens = re.findall(r"[a-zA-Z0-9]+", text.lower())
        if not tokens:
            return SparseVector(indices=[], values=[])
        dim = int(os.environ.get("SPARSE_HASH_DIM", "100000"))
        counts: Dict[int, float] = {}
        for t in tokens:
            hv = int.from_bytes(hashlib.blake2b(t.encode("utf-8"), digest_size=8).digest(), "big") % dim
            counts[hv] = counts.get(hv, 0.0) + 1.0
        indices = list(counts.keys())
        values = list(counts.values())
        return SparseVector(indices=indices, values=values)


# Cross-encoder for reranking
_cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

def rerank_cross_encoder(query: str, docs: List[Dict], top_k: int = 10) -> List[Tuple[Dict, float]]:
    pairs = [(query, d.get("content", "")) for d in docs]
    scores = _cross_encoder.predict(pairs)
    enriched = list(zip(docs, [float(s) for s in scores]))
    enriched.sort(key=lambda x: x[1], reverse=True)
    return enriched[:top_k]