
from nltk import sent_tokenize
import hashlib
import uuid
from unstructured.partition.pdf import partition_pdf

def extract_text_chunks(text: str, max_sentences: int = None):#type:ignore
    """Split a single block of text into smaller segments (sentences)."""
    sentences = sent_tokenize(text)
    chunks = []
    # Option: group e.g. 5 sentences per chunk
    idx = 0
    while idx < len(sentences):
        seg = " ".join(sentences[idx: idx + (max_sentences or len(sentences))])
        chunks.append(seg)
        idx += (max_sentences or len(sentences))
    return chunks


def chunk_blocks(blocks):
    chunks = []
    for b in blocks:
        if b.get("is_table"):
            # table: keep as one chunk
            chunks.append(b)
        else:
            text = b["text"]
            for seg in extract_text_chunks(text, max_sentences=3):
                ch = b.copy()
                ch["text"] = seg
                ch["checksum"] = hashlib.sha256(seg.encode()).hexdigest()
                chunks.append(ch)
    return chunks

