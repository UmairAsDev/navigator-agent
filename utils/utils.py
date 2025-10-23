import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
from dotenv import load_dotenv
from sqlalchemy import select, func
logger = logging.getLogger(__name__)



def _set_env():
    load_dotenv()
    
    required_vars = [
        "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
        "DATABASE_URL", "QDRANT_URL", "QDRANT_COLLECTION_NAME",
        "QDRANT_PORT", "OPENAI_API_KEY", "ACCESS_TOKEN_GOV"
    ]
    
    for var in required_vars:
        if os.getenv(var) is None:
            raise ValueError(f"{var} environment variable not set")
        
async def _get_count(session, model):
    result = await session.execute(select(func.count()).select_from(model))
    return result.scalar_one()




def _log_row_issue(table_name: str, row_idx: int, payload: dict, err: Exception):
    """Log a single row insertion issue with field values and error details."""
    safe_payload = {k: (str(v)[:500] if v is not None else None) for k, v in payload.items()}
    logger.error(
        "Insert failed | table=%s | row=%d | keys=%s | error=%s",
        table_name,
        row_idx,
        {k: safe_payload.get(k) for k in list(safe_payload.keys())[:8]},
        f"{type(err).__name__}: {err}",
    )


