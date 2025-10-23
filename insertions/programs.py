import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from utils.utils import _get_count, _log_row_issue
from pgdatabase.pgdatabase import AsyncSessionLocal


import logging
from datetime import datetime
from schema.models import TariffProgram
logger = logging.getLogger(__name__)







async def insert_tariff_programs(df: pd.DataFrame):
    if df is None or df.empty:
        logger.warning("Tariff DataFrame is empty; skipping tariff insert.")
        return
    df = df.fillna("")
    logger.info("Tariff load start | rows=%d | columns=%s", len(df), list(df.columns))

    if {"tariff_program", "Group"}.issubset(df.columns):
        df = df.drop_duplicates(subset=["tariff_program", "Group"]).reset_index(drop=True)

    async with AsyncSessionLocal() as session:
        before_count = await _get_count(session, TariffProgram)
        data_list = []
        for _, row in df.iterrows():
            data_list.append(
                dict(
                    tariff_program=str(row.get("tariff_program")),
                    Group=str(row["Group"]),
                    Countries=str(row.get("Countries")),
                    description=str(row.get("description")),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )

        stmt = insert(TariffProgram).values(data_list).on_conflict_do_nothing()
        try:
            result = await session.execute(stmt)
            await session.commit()
            logger.info("Tariff bulk insert attempted | rows=%d", len(data_list))
        except Exception as e:
            logger.error("Tariff bulk insert failed: %s", f"{type(e).__name__}: {e}")
            await session.rollback()
            attempted, inserted = 0, 0
            for idx, payload in enumerate(data_list):
                attempted += 1
                try:
                    row_stmt = insert(TariffProgram).values(payload).on_conflict_do_nothing()
                    await session.execute(row_stmt)
                    await session.commit()
                    inserted += 1
                except Exception as re:
                    _log_row_issue("tariff_programs", idx, payload, re)
                    await session.rollback()
            logger.info("Tariff row-by-row insert complete | attempted=%d | inserted=%d", attempted, inserted)

        after_count = await _get_count(session, TariffProgram)
        delta = after_count - before_count
        logger.info("Tariff counts | before=%d | after=%d | delta=%d", before_count, after_count, delta)
