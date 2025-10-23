import os
import sys
import asyncio
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from sqlalchemy import text, select
from pgdatabase.pgdatabase import AsyncSessionLocal
from vectordb.qdrant_db import QdrantDB
from schema.models import HtsCode
import logging


import math

async def filter_with_query(user_input: str):
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
                print(f"database results:{result}")
                if result is None:
                    logging.error(f"Cannot fetch data from Database(postgres)")
                    return []
                rows = result.scalars().all()
                print(f"rows: {rows}")
                db_results = []

                for row in rows:
                    item = row.__dict__.copy()
                    item.pop("_sa_instance_state", None)

                    hts_code = item.get("hts_number")
                    description = item.get("description")
                    specific_rate = item.get("specific_rate_of_duty")
                    column_2 = item.get("column_2_rate_of_duty")
                    general_rate = item.get("general_rate_of_duty")
                    text = item.get("text")

                    spec_levels = [
                        v for k, v in item.items()
                        if k.startswith("spec_level_") and v and str(v).lower() != "nan"
                    ]

                    db_results.append({
                        "hts_code": hts_code,
                        "description": description,
                        "spec_levels": spec_levels,
                        'specific_rate_of_duty':specific_rate,
                        "column2_rate_of_duty":column_2,
                        "general_rate_of_duty":general_rate,
                        "text":text
                    })
                    print(f"fffffffffffffffffffffffff", db_results)

                return db_results


            if isinstance(user_input, str):
                qdrant_db = QdrantDB()
                results = qdrant_db.query_hybrid(query=user_input)
                if results is None:
                    logging.error(f"Cannot Get Data From Qdrant")
                    return []

                vector_results = []
                for item in results:
                    try:
                        payload = item.get("payload", {})
                        metadata = payload.get("metadata", {})

                        hts_code = metadata.get("HTS_Number")
                        description = metadata.get("Description")
                        specific_rate = metadata.get("Specific_Rate_of_Duty")
                        column_2 = metadata.get("Column_2_Rate_of_Duty")
                        general_rate = metadata.get("General_Rate_of_Duty")



                        spec_levels = [
                            v for k, v in metadata.items()
                            if k.startswith("Spec_Level_") and v and v.lower() != "nan"
                        ]

                        vector_results.append({
                            "hts_code": hts_code,
                            "description": description,
                            "spec_levels": spec_levels,
                            'specific_rate_of_duty':specific_rate,
                            "column_rate_of_duty":column_2,
                            "general_rate_of_duty":general_rate
                        })

                    except Exception as e:
                        print(f"Error processing {item.get('id')}: {e}")

                return vector_results
        except Exception as e:
            logging.error(f"cannot get data from both databases{e}")
            return []
        
        


if __name__ == "__main__":
    user_input = "0101300000"
    payloads = asyncio.run(filter_with_query(user_input))
    print(f"payload {payloads}")