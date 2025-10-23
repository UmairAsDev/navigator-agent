import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from utils.utils import _get_count, _log_row_issue
from pgdatabase.pgdatabase import AsyncSessionLocal
import logging
from datetime import datetime
from schema.models import CountryCode
logger = logging.getLogger(__name__)



async def insert_countries(df: pd.DataFrame):
    """Insert countries into the database and vector DB, skipping existing rows."""
    if df is None or df.empty:
        logger.warning("Countries DataFrame is empty; skipping countries insert.")
        return
    df = df.fillna("")
    logger.info("Countries load start | rows=%d | columns=%s", len(df), list(df.columns))

    expected_cols = {"country_name", "iso_2_code", "iso_3_code"}
    if not expected_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {expected_cols}, but found {set(df.columns)}")

    df = df.drop_duplicates(subset=["country_name", "iso_2_code", "iso_3_code"]).reset_index(drop=True)

    async with AsyncSessionLocal() as session:
        before_count = await _get_count(session, CountryCode)
        data_list = []
        for _, row in df.iterrows():
            data_list.append(
                dict(
                    country_name=str(row["country_name"]).strip(),
                    iso_2_code=str(row["iso_2_code"]).strip(),
                    iso_3_code=str(row["iso_3_code"]).strip(),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )

        stmt = insert(CountryCode).values(data_list).on_conflict_do_nothing()
        try:
            await session.execute(stmt)
            await session.commit()
        except Exception as e:
            logger.error("Countries bulk insert failed: %s", f"{type(e).__name__}: {e}")
            await session.rollback()
            for idx, payload in enumerate(data_list):
                try:
                    await session.execute(insert(CountryCode).values(payload).on_conflict_do_nothing())
                    await session.commit()
                except Exception as re:
                    _log_row_issue("country_codes", idx, payload, re)
                    await session.rollback()

        after_count = await _get_count(session, CountryCode)
        delta = after_count - before_count
        logger.info("Countries counts | before=%d | after=%d | delta=%d", before_count, after_count, delta)

        




