import os
import sys
import logging
from datetime import date
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from calculator.query import filter_with_query
from calculator.base_cal import TariffCalculation
from agent.agent_app import agent, MyRunHooks
from agents import Runner


class SearchDataRequest(BaseModel):
    input: str = Field(..., description="Search input")


class TariffCalculationRequest(BaseModel):
    payload: Dict[str, Any]
    country: str = Field(..., description="Country of origin")
    base_cost: float = Field(..., description="Base cost of the goods")
    mode_of_transport: List[str] = Field(..., description="Modes of transport used")
    entry_date: str = Field(..., description="Entry date in YYYY-MM-DD format")
    potential_exclusion_codes: Optional[List[str]] = Field(default=[], description="Potential exclusion codes")


router = APIRouter()


@router.post("/search-data", response_class=JSONResponse)
async def search_data(request: SearchDataRequest):
    """Search HTS or tariff data by input."""
    try:
        data = await filter_with_query(request.input)
        return JSONResponse(content={"status": "success", "payload": data[0]})
    except Exception as e:
        logging.error(f"Error during data search: {e}")
        raise HTTPException(status_code=404, detail="Data not found")






@router.post("/tariff-calculation", response_class=JSONResponse)
async def tariff_calculation(request: TariffCalculationRequest):
    """Calculate total tariff, fees, and total cost for given inputs."""
    try:
        calc = TariffCalculation(
            payload=request.payload,
            country=request.country,
            base_cost=request.base_cost,
            mode_of_transport=request.mode_of_transport,
            entry_date=date.fromisoformat(request.entry_date),
            potential_exclusion_codes=request.potential_exclusion_codes or []
        )
        result = await calc.calculate_total_cost()
        return JSONResponse(content={"status": "success", "data": result})
    except Exception as e:
        logging.error(f"Error during tariff calculation: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    



class PayloadItem(BaseModel):
    hts_code: str
    description: str
    spec_levels: Optional[List[str]] = []
    specific_rate_of_duty: Optional[str] = None
    column2_rate_of_duty: Optional[str] = None
    general_rate_of_duty: Optional[str] = None
    text: Optional[str] = None

class TariffAgentRequest(BaseModel):
    user_input: str = Field(..., description="Search term: HTS code or product description")
    payload: PayloadItem
    base_cost: float
    country: str
    mode_of_transport: List[str]
    entry_date: date
    date_of_loading: date
    potential_exclusion_code: Optional[str] = None

    


@router.post("/tariff-agent", response_class=JSONResponse)
async def tariff_agent(request: TariffAgentRequest):
    """
    Step 2: Use this after selecting a payload from /search-data.
    Performs:
      - Base tariff calculation
      - Total landed cost (MPF/HMF)
      - Additional tariff lookup (Trump 2.0)
    """
    try:
        context = (
            f"Calculate tariff and total cost for product '{request.payload.description}' "
            f"(HTS {request.payload.hts_code}), imported from {request.country}, "
            f"valued at {request.base_cost} USD, via {request.mode_of_transport}, "
            f"entry date {request.entry_date}. "
            f"Include base, program, and additional tariffs."
        )

        agent_result = await Runner.run(agent, context, hooks=MyRunHooks())

        return JSONResponse(
            content={
                "status": "success",
                "input_summary": request.model_dump(),
                "agent_result": agent_result
            }
        )
    except Exception as e:
        logging.error(f"Error running tariff agent: {e}")
        raise HTTPException(status_code=500, detail=str(e))