import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import asyncio
from openai import AsyncOpenAI
from utils.utils import _set_env
_set_env()
client = AsyncOpenAI()
MODEL = "text-embedding-3-large"
async def embed_text(text):
    text = text[:3800]
    res = await client.embeddings.create(model=MODEL, input=text)
    return res.data[0].embedding
async def embed_batch(texts):
    tasks=[embed_text(t) for t in texts]
    return await asyncio.gather(*tasks)
