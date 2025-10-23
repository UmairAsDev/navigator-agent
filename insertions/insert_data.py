import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from insertions.hts import insert_hts_codes
from insertions.programs import insert_tariff_programs
from insertions.country import insert_countries





async def insert_main(hts_df: pd.DataFrame, tariff_df: pd.DataFrame, countries_df: pd.DataFrame):
    await insert_hts_codes(hts_df)
    await insert_tariff_programs(tariff_df)
    await insert_countries(countries_df)