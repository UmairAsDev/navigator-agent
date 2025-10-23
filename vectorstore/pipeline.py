import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import asyncio
from vectorstore.insert import ingest_pdf
from vectorstore.retriever import search

async def main():
    print(await ingest_pdf("/home/umairasdev/Desktop/navigator-agent/pdfs/tariff.pdf"))
    out = await search("section 301 tariffs steel", limit=5)
    for r in out:
        print(r["score"], r["page"], r["content"][:300])

if __name__ == "__main__":
    asyncio.run(main())
