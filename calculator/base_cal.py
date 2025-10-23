import re
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from  sqlalchemy import select
from schema.models import TariffProgram
import asyncio
from typing_extensions import List, Optional, Dict, Any, Union
from pgdatabase.pgdatabase import AsyncSessionLocal
from calculator.query import filter_with_query
import logging
from datetime import date




def parse_duty_rate_string(rate_str: str):
    matches = re.finditer(r"([A-Za-z0-9¢/%\+\.\-\s]*?)\(([^)]*)\)", rate_str)

    program_codes = set()
    program_details = []

    for match in matches:
        prefix = match.group(1).strip()
        inside = match.group(2).strip()

        codes = [c.strip() for c in re.split(r",\s*", inside) if c.strip()]

        for code in codes:
            if re.fullmatch(r"[A-Z]{1,3}\+?|\w\*|\d{4}\.\d{2}\.\d{2}(?:-\d{4}\.\d{2}\.\d{2})?", code):
                program_codes.add(code)
                program_details.append({
                    "program": code,
                    "duty_info": prefix if prefix else "Free"
                })

    return {
        "program_codes": list(program_codes),
        "program_details": program_details
    }





class TariffCalculation:
    """
    Calculates tariffs for imported goods based on country, payload, transport mode,
    entry date, and potential exclusion codes.
    """

    def __init__(
        self,
        payload: Dict[str, Any],
        country: str,
        base_cost: float,
        mode_of_transport: List[str],
        entry_date: date,
        potential_exclusion_codes: Optional[List[str]] = None
    ):
        self.payload = payload
        self.country = country
        self.base_cost = base_cost
        self.mode_of_transport = mode_of_transport
        self.entry_date = entry_date
        self.potential_exclusion_codes = potential_exclusion_codes or []
        
        self.MPF = 35.0  
        self.HMF = 13.0

        self.column_2_countries = {
            "Belarus": {"iso2": "BY", "iso3": "BLR"},
            "Russia": {"iso2": "RU", "iso3": "RUS"},
            "Cuba": {"iso2": "CU", "iso3": "CUB"},
            "North Korea": {"iso2": "KP", "iso3": "PRK"},
        }


    async def calculate_tariff(self) -> Optional[Union[str, Dict[str, Any]]]:
        """Calculate tariff for the given payload and context."""

        specific_rate = self.payload.get("specific_rate_of_duty")


        if self.country in self.column_2_countries:
            column_2_rate = self.payload.get("column2_rate_of_duty")
            if not column_2_rate:
                raise ValueError("column2_rate_of_duty not found in payload")
            return column_2_rate


        if specific_rate and specific_rate:
            parsed_rate = parse_duty_rate_string(specific_rate)
            program_codes = parsed_rate.get("program_codes", [])
            print(f"program_codes{program_codes}")
            program_details = parsed_rate.get("program_details", [])
            print(f"program_details{program_details}")

            if not program_codes or not program_details:
                logging.info("No valid program codes or details found in specific_rate_of_duty")
            else:
                try:
                    async with AsyncSessionLocal() as db:
                        result = await db.execute(
                            select(TariffProgram).where(TariffProgram.tariff_program.in_(program_codes))
                        )
                        program_results = result.scalars().all()

                        if not program_results:
                            logging.info(f"No tariff program matches found for codes: {program_codes}")
                        else:
                            for item in program_results:
                                # print(f"program results{program_results}")
                                data = {col.name: getattr(item, col.name) for col in item.__table__.columns}
                                # print("program results,", data.get('countries'))
                                countries = [c.strip() for c in data.get("countries", "").split(";")]
                                if self.country.strip() in countries:   
                                    print("program countries", countries)
                                    for pd in program_details:
                                        if data.get("tariff_program") == pd.get("program"):
                                            # print("selected program", data.get("tariff_program"))
                                            return {"tariff_program": pd.get("tariff_program"), "duty_info": pd.get("duty_info")}


                except Exception as e:
                    logging.error(f"Error fetching tariff programs: {e}")


        return self.payload.get("general_rate_of_duty")
    
    async def calculate_total_cost(self) -> Dict[str, Any]:
        """Compute total cost including applicable fees and duty logic."""
        tariff = await self.calculate_tariff()

        total_cost = self.base_cost
        applied_fees = []


        tariff_rate = 0.0
        if isinstance(tariff, str):
            tariff_str = tariff.strip().lower()
            if "free" not in tariff_str:

                try:
                    tariff_rate = float(tariff_str.replace("%", ""))
                except ValueError:
                    logging.warning(f"Unrecognized tariff format: {tariff}")
                    tariff_rate = 0.0
        elif isinstance(tariff, (int, float)):
            tariff_rate = float(tariff)


        duty_amount = (self.base_cost * tariff_rate / 100) if tariff_rate > 0 else 0.0
        total_cost += duty_amount

        if "ocean" in [mode.lower() for mode in self.mode_of_transport]:
            total_cost += self.MPF + self.HMF
            applied_fees.extend(["MPF", "HMF"])
        else:
            total_cost += self.MPF
            applied_fees.append("MPF")

        return {
            "country": self.country,
            "base_cost": self.base_cost,
            "tariff_rate": f"{tariff_rate}%" if tariff_rate else "Free",
            "duty_amount": round(duty_amount, 2),
            "applied_fees": applied_fees,
            "total_cost": round(total_cost, 2)
        }
                    
                    



        
if __name__ == "__main__":
    async def main():
        input_val = "0101210010"
        payload_list = await filter_with_query(input_val)
        print(f"payload_list: {payload_list}")

        if not payload_list:
            print("No payload received, cannot calculate tariff.")
            return

        selected_payload = payload_list[0]
        print(f"selected payload", selected_payload)
        calculator = TariffCalculation(
            payload=selected_payload,
            country="Afghanistan",
            base_cost=1000,
            mode_of_transport=["Air"],
            entry_date=date(2023, 1, 1)
        )
        result = await calculator.calculate_tariff()
        
        print(result)

    asyncio.run(main())