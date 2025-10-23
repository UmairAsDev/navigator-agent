import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import asyncio
import logging
from datetime import datetime
from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert

from schema.models import TariffProgram, CountryCode
from pgdatabase.pgdatabase import AsyncSessionLocal

from scraper.scrap_countries import countries_list
from scraper.scrap_programs import preprocess_tariff_programs
from scraper.scrap_hts import run_scraper
from insertions.insert_data import insert_main


logger = logging.getLogger(__name__)


async def ensure_countries_inserted():
    """Fetch countries from API and insert into DB (idempotent, DB-only)."""
    try:
        df = countries_list()
        if df is None or df.empty:
            logger.warning("Countries API returned empty; skipping.")
            return
        df = df.fillna("")
        df = df.drop_duplicates(subset=["country_name", "iso_2_code", "iso_3_code"]).reset_index(drop=True)

        async with AsyncSessionLocal() as session:
            before = await session.execute(select(func.count()).select_from(CountryCode))
            before_count = before.scalar() or 0

            data_list = []
            for _, row in df.iterrows():
                data_list.append(
                    dict(
                        country_name=str(row["country_name"]).strip(),
                        iso_2_code=str(row["iso_2_code"]).strip(),
                        iso_3_code=str(row["iso_3_code"]).strip(),
                    )
                )

            stmt = insert(CountryCode).values(data_list).on_conflict_do_nothing()
            await session.execute(stmt)
            await session.commit()

            after = await session.execute(select(func.count()).select_from(CountryCode))
            after_count = after.scalar() or 0
        logger.info(
            "Countries inserted | before=%d | after=%d | delta=%d",
            before_count,
            after_count,
            after_count - before_count,
        )
    except Exception as e:
        logger.error("Countries insert failed: %s", f"{type(e).__name__}: {e}")


async def dedupe_tariff_programs_db():
    """Remove duplicate tariff program rows keeping the lowest id per (tariff_program, Group)."""
    try:
        async with AsyncSessionLocal() as session:
            before = await session.execute(select(func.count()).select_from(TariffProgram))
            before_count = before.scalar() or 0

            del_sql = text(
                'DELETE FROM public.tariff_programs t USING public.tariff_programs d '
                'WHERE t.tariff_program = d.tariff_program AND t."Group" = d."Group" AND t.id > d.id;'
            )
            await session.execute(del_sql)
            await session.commit()

            after = await session.execute(select(func.count()).select_from(TariffProgram))
            after_count = after.scalar() or 0

            removed = before_count - after_count
            logger.info("Tariff de-dup complete | before=%d | after=%d | removed=%d", before_count, after_count, removed)
    except Exception as e:
        logger.error("Tariff de-dup failed: %s", f"{type(e).__name__}: {e}")






async def main_async():
    logger.info("Cron run started")
    start = datetime.now()


    hts_df = run_scraper()
    logger.info("HTS scraped and processed | rows=%d", len(hts_df))

    tariff_df = preprocess_tariff_programs()
    logger.info("Tariff programs processed | rows=%d", len(tariff_df))

    countries_df = countries_list()
    logger.info("Countries processed | rows=%d", len(countries_df))

    await insert_main(hts_df, tariff_df, countries_df)

    await ensure_countries_inserted()
    await dedupe_tariff_programs_db()

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("Cron run completed | elapsed=%.2fs", elapsed)



def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
