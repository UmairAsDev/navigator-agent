import os
import sys
import uuid
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Tuple, Dict, Optional
from concurrent.futures import ThreadPoolExecutor

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    PointStruct,
    Filter,
    NamedSparseVector,
)
from vectordb.embed import create_dense_embedding, create_sparse_embedding, rerank_cross_encoder
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from qdrant_client.http.exceptions import ResponseHandlingException
logger = logging.getLogger(__name__)



USE_FAST = os.getenv("QDRANT_FAST", "0") == "1"

DENSE_SIZE = 1536


class QdrantDB:
    def __init__(self, collection_name: str = "navigator_sparse"):
        self.collection_name = collection_name
        self.client = QdrantClient(host="localhost", port=6333)


        retries = 3
        for attempt in range(retries):
            try:
                if not self.client.collection_exists(self.collection_name):
                    self._create_collection()
                break
            except ResponseHandlingException as e:
                if attempt < retries - 1:
                    logger.warning("Retrying collection_exists due to timeout (attempt %d/%d)...", attempt + 1, retries)
                    time.sleep(2 ** attempt)  
                else:
                    logger.error("Failed to check collection existence after %d attempts | error=%s", retries, f"{type(e).__name__}: {e}")
                    raise

    def _create_collection(self):
        logger.info("Creating collection '%s'", self.collection_name)
        self.client.create_collection(self.collection_name)

    def _normalize_point_id(self, point_id):
        """Normalize point ID to ensure it is either an unsigned integer or a valid UUID."""
        try:
            if isinstance(point_id, int):
                if point_id < 0:
                    raise ValueError("Point ID must be an unsigned integer.")
                return point_id
    
            if isinstance(point_id, str):
                try:
                    return str(uuid.UUID(point_id))
                except ValueError:
                    return str(uuid.uuid5(uuid.NAMESPACE_DNS, point_id))
    
            raise ValueError("Point ID must be either an unsigned integer or a valid UUID.")
        except Exception as e:
            logger.error("Failed to normalize point ID '%s' | error=%s", point_id, f"{type(e).__name__}: {e}")
            raise

    def _create_dense(self, content: str, fast: bool = False) -> List[float]:
        if fast or USE_FAST:
            return [0.0] * DENSE_SIZE
        return create_dense_embedding(content)

    def add_documents(self, documents: List[tuple], fast: bool = False, batch_size: int = 64, timeout: int = 30):
        logger.info("Starting to add documents in batches. Total documents: %d", len(documents))
        start_time = time.time()
    
        def process_batch(batch):
            ids, payloads, dense_vectors = [], [], []
    
            for doc_id, content, metadata in batch:
                try:
                    normalized_id = self._normalize_point_id(doc_id)
                    ids.append(str(normalized_id))
    
                    dense_vector = self._create_dense(content, fast=fast)
                    dense_vectors.append(dense_vector)
    
                    payloads.append({
                        "content": content,
                        "metadata": metadata,
                    })
                except Exception as e:
                    logger.error("Failed to process document | error=%s", f"{type(e).__name__}: {e}")
    
            try:
                retries = 3
                for attempt in range(retries):
                    try:
                        self.client.upsert(
                            collection_name=self.collection_name,
                            points=[
                                PointStruct(
                                    id=doc_id,
                                    payload=payload,
                                    vector={"vector": dense_vector},
                                )
                                for doc_id, payload, dense_vector in zip(ids, payloads, dense_vectors)
                            ],
                        )
                        logger.info("Successfully added batch of %d documents to collection '%s'", len(batch), self.collection_name)
                        break
                    except ResponseHandlingException as e:
                        if attempt < retries - 1:
                            logger.warning("Retrying batch due to timeout (attempt %d/%d)...", attempt + 1, retries)
                            time.sleep(2 ** attempt)  
                        else:
                            logger.error("Failed to add batch after %d attempts | error=%s", retries, f"{type(e).__name__}: {e}")
            except Exception as e:
                logger.error("Failed to add batch | error=%s", f"{type(e).__name__}: {e}")
    
        with ThreadPoolExecutor(max_workers=4) as executor:
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                executor.submit(process_batch, batch)
    
        logger.info("Finished adding documents in batches. Total time: %.2f seconds", time.time() - start_time)

    def add_documents_if_new(self, documents: List[Tuple[str, str, dict]], fast: bool = False, workers: int = 1) -> int:
        if not documents:
            logger.info("No documents provided for insertion.")
            return 0
        ids = [self._normalize_point_id(doc_id) for doc_id, _, _ in documents]
        existing_ids = set()
        logger.info("Checking for existing documents in collection '%s'.", self.collection_name)
        start_time = time.time()
    
        # Batch retrieve to avoid large payloads
        BATCH = 128
        for i in range(0, len(ids), BATCH):
            batch = ids[i:i+BATCH]
            try:
                results = self.client.retrieve(collection_name=self.collection_name, ids=batch)#type:ignore
                for pt in results:
                    try:
                        existing_ids.add(str(pt.id))
                    except Exception:
                        pass
            except Exception:
                logger.warning("Failed to retrieve existing documents for batch %d-%d", i, i+BATCH)
                continue
    
        new_docs = [(doc_id, content, meta) for (doc_id, content, meta) in documents if str(self._normalize_point_id(doc_id)) not in existing_ids]
        if not new_docs:
            logger.info("No new documents to add.")
            return 0
        logger.info("Adding %d new documents to collection '%s'.", len(new_docs), self.collection_name)
        self.add_documents(new_docs, fast=fast)
        logger.info("Successfully added %d new documents to collection '%s'. Total time: %.2f seconds", len(new_docs), self.collection_name, time.time() - start_time)
        return len(new_docs)

    def query_hybrid(
    self,
    query: str,
    limit: int = 10,
    qfilter: Optional[Filter] = None,
    rrf_k: int = 60,
    rerank_top_k: Optional[int] = None,
    blend_ratio: float = 0.3,
    ) -> List[Dict]:
        """
        Production-grade hybrid search:
        Combines dense + sparse search with RRF, normalizes scores,
        then optionally reranks top-k with a cross-encoder.
        """

        try:
            dense_results = self.client.search(
                collection_name=self.collection_name,
                query_vector=("vector", create_dense_embedding(query)),
                limit=limit,
                with_payload=True,
                query_filter=qfilter,
            )
        except Exception as e:
            print(f"[DenseSearchError] {e}")
            dense_results = []

        try:
            sparse_results = self.client.search(
                collection_name=self.collection_name,
                query_vector=NamedSparseVector(name="sparse", vector=create_sparse_embedding(query)),
                limit=limit,
                with_payload=True,
                query_filter=qfilter,
            )
        except Exception as e:
            print(f"[SparseSearchError] {e}")
            sparse_results = []

        def normalize(results):
            if not results: return results
            scores = [r.score for r in results]
            min_s, max_s = min(scores), max(scores)
            scale = (max_s - min_s) or 1e-6
            for r in results:
                r.score = (r.score - min_s) / scale
            return results

        dense_results = normalize(dense_results)
        sparse_results = normalize(sparse_results)

        def rrf_scores(results):
            scores = {}
            for rank, r in enumerate(results, start=1):
                scores[str(r.id)] = scores.get(str(r.id), 0.0) + 1.0 / (rrf_k + rank)
            return scores

        rrf = {}
        for s in [rrf_scores(dense_results), rrf_scores(sparse_results)]:
            for sid, val in s.items():
                rrf[sid] = rrf.get(sid, 0.0) + val

        merged = {}
        for result_set in [dense_results, sparse_results]:
            for r in result_set:
                sid = str(r.id)
                merged[sid] = {
                    "id": sid,
                    "score": float(rrf.get(sid, 0.0)),
                    "payload": r.payload,
                }

        fused = sorted(merged.values(), key=lambda x: x["score"], reverse=True)[:limit]

        if rerank_top_k and rerank_top_k > 0:
            try:
                from sentence_transformers import CrossEncoder
                model = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2", device="cuda")

                docs_for_rerank = [
                    {
                        "text": f"{d['payload'].get('title', '')}. {d['payload'].get('content', '')}",
                        **d
                    }
                    for d in fused[:rerank_top_k]
                ]

                pairs = [(query, d["text"]) for d in docs_for_rerank]
                scores = model.predict(pairs, batch_size=16)

                for i, d in enumerate(docs_for_rerank):
                    d["rerank_score"] = float(scores[i])
                    d["final_score"] = (
                        (1 - blend_ratio) * d["score"] + blend_ratio * d["rerank_score"]
                    )

                fused = sorted(docs_for_rerank, key=lambda x: x["final_score"], reverse=True)
            except Exception as e:
                print(f"[RerankError] {e}")

        return fused
