import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
import logging
import asyncio
import json
from datetime import date
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
from agents import function_tool
from sqlalchemy import select
from pgdatabase.pgdatabase import AsyncSessionLocal
from vectordb.qdrant_db import QdrantDB
from schema.models import HtsCode
from calculator.base_cal import TariffCalculation
from vectorstore.retriever import search


logging.basicConfig(level=logging.INFO)

@function_tool
async def retrieve_payloads(user_input: str) -> List[Dict[str, Any]]:
    logging.info(f"[retrieve_payloads] input: {user_input}")
    async with AsyncSessionLocal() as db:
        try:
            if user_input.isdigit():
                print(f"user_input: {user_input.strip().replace('.', '')}")
                result = await db.execute(
                    select(HtsCode).where(
                        HtsCode.hts_digits.startswith(
                            f"{user_input.strip().replace('.', '')}"
                        )
                    )
                )
                rows = result.scalars().all()
                print(f"rows: {rows}")
                out = []
                for row in rows:
                    item = row.__dict__.copy()
                    item.pop("_sa_instance_state", None)
                    spec_levels = [
                        v for k, v in item.items()
                        if k.startswith("spec_level_") and v and str(v).lower() != "nan"
                    ]
                    out.append({
                        "hts_code": item.get("hts_digits"),
                        "description": item.get("description"),
                        "spec_levels": spec_levels,
                        "specific_rate_of_duty": item.get("specific_rate_of_duty"),
                        "column2_rate_of_duty": item.get("column2_rate_of_duty"),
                        "general_rate_of_duty": item.get("general_rate_of_duty"),
                        "text":item.get("text")
                    })
                logging.info(f"[retrieve_payloads] returned {len(out)} payloads")
                return out
            else:
                qdrant_db = QdrantDB()
                results = qdrant_db.query_hybrid(query=user_input)
                if results is None:
                    logging.error("[retrieve_payloads] Qdrant returned None")
                    return []
                out = []
                for item in results:
                    try:
                        payload = item.get("payload", {})
                        metadata = payload.get("metadata", {})
                        spec_levels = [
                            v for k, v in metadata.items()
                            if k.startswith("Spec_Level_") and v and str(v).lower() != "nan"
                        ]
                        out.append({
                            "hts_code": metadata.get("HTS_Number"),
                            "description": metadata.get("Description"),
                            "spec_levels": spec_levels,
                            "specific_rate_of_duty": metadata.get("Specific_Rate_of_Duty"),
                            "column2_rate_of_duty": metadata.get("Column_2_Rate_of_Duty"),
                            "general_rate_of_duty": metadata.get("General_Rate_of_Duty")
                        })
                    except Exception as e:
                        logging.error(f"[retrieve_payloads] error processing vector item: {e}")
                logging.info(f"[retrieve_payloads] returned {len(out)} vector payloads")
                return out
        except Exception as e:
            logging.error(f"[retrieve_payloads] failed: {e}")
            return []



class PayloadItem(BaseModel):
    hts_code: Optional[str]
    description: Optional[str]
    spec_levels: Optional[List[str]]
    specific_rate_of_duty: Optional[str]
    column2_rate_of_duty: Optional[str]
    general_rate_of_duty: Optional[str]
    text: Optional[str]


class CalculateTariff(BaseModel):
    payload: List[PayloadItem]
    country: str
    base_cost: float
    mode_of_transport: List[str]
    entry_date: date


@function_tool(strict_mode=False)
async def calculate_tariff_for_countries(request: CalculateTariff) -> dict:
    """Calculates the tariff for imported goods based on specific criteria."""
    try:
        payload = request.payload[0].dict()
        print(f"payload: {payload}")
        calculator = TariffCalculation(
            payload=payload,
            country=request.country,
            base_cost=request.base_cost,
            mode_of_transport=request.mode_of_transport,
            entry_date=request.entry_date,
        )
        result = await calculator.calculate_tariff()
        return {"country_tool_result": result}
    except Exception as e:
        logging.error(f"calculate_tariff_for_countries failed: {e}")
        return {"error": str(e)}


class CostBreakdown(BaseModel):
    payload: Optional[List[PayloadItem]] = None  
    country: str
    base_cost: float
    mode_of_transport: List[str]
    entry_date: date


@function_tool(strict_mode=False)
async def calculate_total_cost(request: CostBreakdown) -> dict:
    """Computes total landed cost, including MPF/HMF fees and duty."""
    try:
        payload = request.payload[0].dict() if request.payload else {}

        calculator = TariffCalculation(
            payload=payload,
            country=request.country,
            base_cost=request.base_cost,
            mode_of_transport=request.mode_of_transport,
            entry_date=request.entry_date,
        )
        return await calculator.calculate_total_cost()
    except Exception as e:
        logging.error(f"calculate_total_cost failed: {e}")
        return {"error": str(e)}

class AdvanceTariff(BaseModel):
    query :str 



@function_tool
async def addtional_tariff_check(request: AdvanceTariff):
    """
    Searches the tariff tracker for additional or reciprocal tariffs.
    Returns the raw results enriched with all textual metadata,
    excluding only invalid or non-textual fields (e.g. image_url, file_id).
    """
    results = await search(request.query)
    cleaned_results = []

    for r in results:
        meta = r.get("meta", {}) or {}

        safe_meta = {
            k: v
            for k, v in meta.items()
            if k not in ("image_url", "file_id", "thumbnail", "embedding", "vector","doc_source")
        }

        cleaned_results.append({
            "id": r.get("id"),
            "score": float(r.get("score", 0.0)),
            "content": r.get("content"),
            "meta": safe_meta,
            "page": r.get("page"),
            "checksum": r.get("checksum"),
            "is_table": r.get("is_table", False)
        })

    return json.dumps(cleaned_results)





# if __name__ == "__main__":
#     async def main():
#         input_val = "cotton knitted shirt"
#         payload_list = await retrieve_payloads(input_val)
#         print(f"payload_list: {payload_list}")

#         if not payload_list:
#             print("No payload received, cannot calculate tariff.")
#             return
#         selected_payload = payload_list[3]
#         print(f"selected_payload: {selected_payload}")
#         calculator = TariffCalculation(
#             payload=selected_payload,
#             country="Uganda",
#             base_cost=1000,
#             mode_of_transport=["Air"],
#             entry_date=date(2023, 1, 1)
#         )
#         result = await calculator.calculate_tariff()
#         print(result)
#         total_cost = await calculator.calculate_total_cost()
#         print(total_cost)

#     asyncio.run(main())