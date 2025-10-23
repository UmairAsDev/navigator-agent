import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import asyncio
import ssl
import logging
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import OperationalError
from typing import AsyncIterator
from sqlalchemy import text, MetaData
from utils.utils import _set_env
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


    

_set_env()



database_url = os.getenv("DATABASE_URL")
if not database_url:
    raise ValueError("DATABASE_URL environment variable is not set")


ssl_context = ssl.create_default_context()


Base = declarative_base()
engine = create_async_engine(
    database_url,
    echo=False,
)


AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession
)




async def init_db(drop: bool = False):
    """Initialize database schema from models."""
    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schema initialized")




async def get_db()-> AsyncIterator[AsyncSession]:
    async with AsyncSessionLocal() as session:
        try:
            logger.info("Database connection established")
            yield session
        except OperationalError as e:
            logger.error(f"Database error: {e}")
            raise
        
async def test_connection():
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT * FROM public.hts_codes WHERE hts_digits LIKE '01013%';"))
            row = result.fetchone()
            logger.info("Connection OK: %s", row)
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")

if __name__ == "__main__":
    import sys
    
    async def main():   
        if "init" in sys.argv:
            await init_db(drop=False)
        elif "test" in sys.argv:
            await test_connection()
    
    asyncio.run(main())
    
    logger.info("Database connection test completed")



