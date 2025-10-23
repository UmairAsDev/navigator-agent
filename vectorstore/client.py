import os, time, uuid, json, logging, asyncio, hashlib
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as rest
from qdrant_client.http.models import VectorParams, Distance, PointStruct, VectorsConfig
from qdrant_client.http.exceptions import ResponseHandlingException

BACKUP_DIR = os.getenv("QDRANT_BACKUP_DIR","./qdrant_backups")
Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
logger = logging.getLogger(__name__)

def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


qdrant_url = "https://3263bd08-1ccd-4714-aead-51d975df75e1.us-west-2-0.aws.cloud.qdrant.io:6333"
qdrant_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwiZXhwIjoxNzY5Nzk0MDU4fQ.PdqsxAYyGWriMiyQMBUOEjqzW4n3L_AyfNYQdOcmFpc"

from qdrant_client import AsyncQdrantClient

class QdrantClient:
    def __init__(self, qdrant_url: str, qdrant_api_key: str, collection_name: str = "tariff_docs", dim:int = 3072):
        self.client = AsyncQdrantClient(url=qdrant_url, api_key=qdrant_api_key)
        self.collection = collection_name
        self.dim = dim
        self.vector_name = "vector"
        

    async def ensure_collection(self):
        try:
            # Check if the collection exists
            collections = await self.client.get_collections()
            if self.collection not in [col.name for col in collections.collections]:
                print(f"Creating collection '{self.collection}' with vector size {self.dim}...")
                
                await self.client.create_collection(
                    collection_name=self.collection,
                    vectors_config={
                        "vector": VectorParams(
                            size=self.dim, 
                            distance=Distance.COSINE
                        )
                    }
                
                )
                
        except Exception as e:
            print(f"Error ensuring collection: {e}")
    async def _backup(self, items, prefix="upsert"):
        path = Path(BACKUP_DIR)/f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:6]}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for i in items: fh.write(json.dumps(i, ensure_ascii=False)+"\n")
        return str(path)

    async def upsert_documents(self, docs: List[Tuple[str,str,Dict[str,Any]]], vectors: List[List[float]], batch_size=64, retries=3):
        assert len(docs)==len(vectors)
        for i in range(0,len(docs),batch_size):
            batch_docs = docs[i:i+batch_size]; batch_vecs = vectors[i:i+batch_size]
            points=[]; backup_items=[]
            for (doc_id, text, meta), vec in zip(batch_docs, batch_vecs):
                pid = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(doc_id)))
                payload = {
                    "doc_id": doc_id,
                    "content": text,
                    "checksum": meta.get("checksum") or _checksum(text),
                    "page": meta.get("page"),
                    "category": meta.get("category"),
                    "is_table": meta.get("is_table", False),
                    "meta": meta.get("metadata", {})
                }
                if not isinstance(vec, list) or len(vec)!=self.dim:
                    logger.warning("skip %s vector-dim mismatch", doc_id); continue
                points.append(PointStruct(id=pid, vector={self.vector_name: vec}, payload=payload))
                backup_items.append({"id":pid,"payload":payload})
            if not points: continue
            await self._backup(backup_items,"upsert")
            attempt=0
            while attempt<retries:
                try:
                    await self.client.upsert(collection_name=self.collection, points=points)
                    break
                except ResponseHandlingException as e:
                    attempt+=1; await asyncio.sleep(2**attempt)
                except Exception as e:
                    logger.exception("upsert unexpected: %s",e); break

    async def add_chunks_if_new(self, chunks: List[Dict[str,Any]], vectors: List[List[float]]):
        checks = [c.get("checksum") or _checksum(c.get("text","")) for c in chunks]
        found=set()
        B=64
        for i in range(0,len(checks),B):
            sub=checks[i:i+B]
            try:
                qf = rest.Filter(must=[rest.FieldCondition(key="checksum", match=rest.MatchValue(any=sub))])#type:ignore
                res = await self.client.search(collection_name=self.collection, query_vector=(self.vector_name,[0.0]*self.dim), limit=1, query_filter=qf, with_payload=True)
                for r in res:
                    cs=r.payload.get("checksum"); #type:ignore
                    if cs: found.add(cs)
            except Exception:
                pass
        new_docs=[]; new_vecs=[]
        for c,v,cs in zip(chunks,vectors,checks):
            if cs in found: continue
            meta={"checksum":cs,"page":c.get("page"),"category":c.get("category"),"is_table":c.get("is_table"),"metadata":c.get("metadata",{})}
            new_docs.append((c.get("doc_id") or cs, c.get("text",""), meta)); new_vecs.append(v)
        if not new_docs: return 0
        await self.upsert_documents(new_docs,new_vecs)
        return len(new_docs)

    async def hybrid_query(self, vector: List[float], limit:int=15, qfilter:Optional[rest.Filter]=None):
        res = await self.client.search(collection_name=self.collection, query_vector=(self.vector_name,vector), limit=limit, with_payload=True, query_filter=qfilter)
        out=[]
        for r in res:
            out.append({
                "id":str(r.id),"score":float(r.score),"content":r.payload.get("content"),"meta":r.payload.get("meta"),"page":r.payload.get("page"),"checksum":r.payload.get("checksum"),"is_table":r.payload.get("is_table")#type:ignore
            })
        return out

    async def get_point(self, pid:str):
        try:
            pts = await self.client.retrieve(collection_name=self.collection, ids=[pid])
            if not pts: return None
            p=pts[0]; return {"id":str(p.id),"payload":p.payload}
        except Exception:
            return None