import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import re
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
from typing import List, Optional, Union
import logging
import io


ARCHIVE_URL = "https://www.usitc.gov/harmonized_tariff_information/hts/archive/list"
HTS_CODE_REGEX = re.compile(r"\b(\d{10})\b")

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _find_csv_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Extract CSV file links from an HTML page."""
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        text = (a.get_text() or "").lower()
        if href and (href.lower().endswith(".csv") or "csv" in text):#type:ignore
            links.append(urljoin(base_url, href))#type:ignore
    return links


def download_csv_via_requests(list_page_url: str) -> str:
    """Find and return the latest CSV file link from the archive page."""
    logger.info(f"Fetching archive list from {list_page_url} ...")
    resp = requests.get(list_page_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    csv_links = _find_csv_links(soup, list_page_url)
    if not csv_links:
        raise RuntimeError("No CSV links found on the archive page.")
    latest_csv = csv_links[0]
    logger.info(f"Found latest CSV: {latest_csv}")
    return latest_csv


def download_latest_hts_csv_bytes() -> bytes:
    """Download the latest HTS CSV and return raw bytes without saving to disk."""
    csv_url = download_csv_via_requests(ARCHIVE_URL)
    logger.info("Downloading HTS CSV into memory")
    resp = requests.get(csv_url, timeout=60)
    resp.raise_for_status()
    return resp.content


def preprocess_hts_csv(csv_input: Union[Path, pd.DataFrame, bytes], max_levels: int = 10) -> pd.DataFrame:
    """
    Flatten HTS CSV into a structured DataFrame:
      - Expand hierarchy into Spec_Level_1...Spec_Level_N
      - Inherit duty rates and unit of quantity from parent levels.
    Accepts a Path, an in-memory DataFrame, or raw CSV bytes.
    """
    logger.info("Preprocessing HTS CSV in memory")

    if isinstance(csv_input, pd.DataFrame):
        df = csv_input.copy()
    elif isinstance(csv_input, (bytes, bytearray)):
        df = pd.read_csv(io.BytesIO(csv_input))
    else:
        df = pd.read_csv(csv_input)#type:ignore

    df.columns = [c.strip() for c in df.columns]

    def extract_digits(s: Optional[str]) -> str:
        return ''.join(re.findall(r'\d', str(s))) if s else ''

    current_levels = [''] * (max_levels + 1)
    duty_per_level = [{"General": "", "Special": "", "Column2": "", "Unit": ""} for _ in range(max_levels + 1)]

    out_rows = []

    for _, row in df.iterrows():
        desc = str(row.get('Description', '')).strip()
        indent_str = str(row.get('Indent', '')).strip()

        try:
            indent = int(indent_str) if indent_str else None
        except ValueError:
            indent = None


        if indent is not None:
            indent = max(0, min(indent, max_levels))


        if indent is not None and desc:
            current_levels[indent] = desc
            for i in range(indent + 1, max_levels + 1):
                current_levels[i] = ""
                duty_per_level[i] = {"General": "", "Special": "", "Column2": "", "Unit": ""}


        if indent is not None:
            for key, col in {
                "General": "General Rate of Duty",
                "Special": "Special Rate of Duty",
                "Column2": "Column 2 Rate of Duty",
                "Unit": "Unit of Quantity"
            }.items():
                val = str(row.get(col, "")).strip()
                if val:
                    duty_per_level[indent][key] = val

        def get_effective_value(key: str) -> str:
            if indent is None:
                return ""
            for lvl in range(indent, -1, -1):
                val = duty_per_level[lvl][key]
                if val:
                    return val
            return ""

        raw_hts = row.get('HTS Number', '')
        digits = extract_digits(raw_hts)

        if len(digits) >= 10:
            out = {
                "HTS_Number": raw_hts,
                "HTS_Digits": digits[:10],
                "Indent": indent or "",
                "Description": current_levels[0],
            }
            for lvl in range(1, max_levels + 1):
                out[f"Spec_Level_{lvl}"] = current_levels[lvl]

            out.update({
                "Unit_of_Quantity": get_effective_value("Unit"),
                "General_Rate_of_Duty": get_effective_value("General"),
                "Special_Rate_of_Duty": get_effective_value("Special"),
                "Column_2_Rate_of_Duty": get_effective_value("Column2"),
            })
            out_rows.append(out)

    out_df = pd.DataFrame(out_rows)

    if out_df.empty:
        raise RuntimeError("No valid HTS rows were parsed from the CSV.")


    out_df = out_df.loc[:, (out_df != "").any(axis=0)]


    spec_cols = [c for c in out_df.columns if c.startswith("Spec_Level_")]

    def build_text(row):
        parts = [str(row.get(c, "")).strip() for c in spec_cols if str(row.get(c, "")).strip()]
        hts = row.get("HTS_Digits", "")
        prefix4, prefix6 = hts[:4], hts[:6]
        meta = [f"prefix4:{prefix4}", f"prefix6:{prefix6}"] if hts else []
        return " | ".join(meta + parts)

    out_df["text"] = out_df.apply(build_text, axis=1)
    logger.info(f"Processed HTS records: {len(out_df):,}")

    return out_df


def run_scraper() -> pd.DataFrame:
    """Run the complete scraper and return processed DataFrame without writing to disk."""
    raw = download_latest_hts_csv_bytes()
    processed_df = preprocess_hts_csv(raw)
    return processed_df


# if __name__ == "__main__":
#     df = run_scraper()
#     logger.info(f"Final processed HTS rows: {len(df):,}")
#     print(df.head(5))
