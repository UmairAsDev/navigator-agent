import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging 
import datetime
from datetime import datetime
import pandas as pd
import numpy as np
from schema.models import HtsCode
from sqlalchemy.dialects.postgresql import insert
from utils.utils import _get_count, _log_row_issue
from pgdatabase.pgdatabase import AsyncSessionLocal
logger = logging.getLogger(__name__)




async def insert_hts_codes(df: pd.DataFrame):
    """Insert HTS codes from a processed DataFrame and sync to vector DB, skipping existing records."""
    if df is None or df.empty:
        logger.warning("HTS DataFrame is empty; skipping HTS insert.")
        return

    df = df.replace({np.nan: None})
    logger.info("HTS load start | rows=%d | columns=%s", len(df), list(df.columns))

    required_cols = {"HTS_Number", "HTS_Digits", "Description"}
    missing = required_cols - set(df.columns)
    if missing:
        logger.error("HTS CSV missing required columns: %s", missing)
        return

    async with AsyncSessionLocal() as session:
        before_count = await _get_count(session, HtsCode)
        data_list = []

        for _, row in df.iterrows():
            indent_val = row.get("Indent", 0)
            try:
                indent_parsed = int(indent_val) if str(indent_val).strip() != "" else 0
            except (ValueError, TypeError):
                indent_parsed = 0

            payload = dict(
                HTS_Number=row.get("HTS_Number"),
                HTS_Digits=row.get("HTS_Digits"),
                Indent=indent_parsed,
                Description=row.get("Description"),
                General_Rate_of_Duty=row.get("General_Rate_of_Duty"),
                Specific_Rate_of_Duty=row.get("Specific_Rate_of_Duty") or row.get("Special_Rate_of_Duty"),
                Column_2_Rate_of_Duty=row.get("Column_2_Rate_of_Duty"),
                Unit_of_Quantity=row.get("Unit_of_Quantity"),
                Spec_Level_1=row.get("Spec_Level_1"),
                Spec_Level_2=row.get("Spec_Level_2"),
                Spec_Level_3=row.get("Spec_Level_3"),
                Spec_Level_4=row.get("Spec_Level_4"),
                Spec_Level_5=row.get("Spec_Level_5"),
                Spec_Level_6=row.get("Spec_Level_6"),
                Spec_Level_7=row.get("Spec_Level_7"),
                Spec_Level_8=row.get("Spec_Level_8"),
                Spec_Level_9=row.get("Spec_Level_9"),
                Spec_Level_10=row.get("Spec_Level_10"),
                text=row.get("text"),
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )

            if len(data_list) < 5:
                logger.debug("HTS sample row | %s", {k: payload.get(k) for k in ["HTS_Number", "HTS_Digits", "Indent", "Description"]})

            data_list.append(payload)

        batch_size = 500
        total_inserted = 0
        try:
            for i in range(0, len(data_list), batch_size):
                chunk = data_list[i:i + batch_size]
                stmt = insert(HtsCode).values(chunk).on_conflict_do_nothing()
                await session.execute(stmt)
                total_inserted += len(chunk)
            await session.commit()
            logger.info("HTS bulk insert attempted | rows=%d", total_inserted)
        except Exception as e:
            logger.error("HTS bulk insert failed: %s", f"{type(e).__name__}: {e}")
            await session.rollback()
            attempted, inserted = 0, 0
            for idx, payload in enumerate(data_list):
                attempted += 1
                try:
                    row_stmt = insert(HtsCode).values(payload).on_conflict_do_nothing()
                    await session.execute(row_stmt)
                    await session.commit()
                    inserted += 1
                except Exception as re:
                    _log_row_issue("hts_codes", idx, payload, re)
                    await session.rollback()
            logger.info("HTS row-by-row insert complete | attempted=%d | inserted=%d", attempted, inserted)

        after_count = await _get_count(session, HtsCode)
        delta = after_count - before_count
        logger.info("HTS counts | before=%d | after=%d | delta=%d", before_count, after_count, delta)
