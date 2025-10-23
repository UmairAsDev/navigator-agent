import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pdf_extractor.tariff_tracker import extract_document
from vectorstore.chunker import chunk_blocks
from vectorstore.embedings import embed_batch
from vectorstore.client import QdrantClient
from pdf2image.exceptions import PDFPageCountError

qdrant_url = "https://3263bd08-1ccd-4714-aead-51d975df75e1.us-west-2-0.aws.cloud.qdrant.io:6333"
qdrant_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwiZXhwIjoxNzY5Nzk0MDU4fQ.PdqsxAYyGWriMiyQMBUOEjqzW4n3L_AyfNYQdOcmFpc"
async def ingest_pdf(pdf_path: str):
    try:
        blocks = extract_document(pdf_path)
    except Exception as e:
        print(f"Failed to extract_document for {pdf_path}: {e}")
        return None

    if not blocks:
        print(f"No blocks found in {pdf_path}, skipping.")
        return None

    chunks = chunk_blocks(blocks)
    if not chunks:
        print(f"No chunks produced for {pdf_path}, skipping.")
        return None

    texts = [c["text"] for c in chunks]
    vectors = await embed_batch(texts)

    q = QdrantClient(qdrant_url=qdrant_url, qdrant_api_key=qdrant_api_key)
    await q.ensure_collection()

    added = await q.add_chunks_if_new(chunks, vectors)

    return {"chunks": len(chunks), "added": added}
