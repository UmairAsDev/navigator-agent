import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".../..")))
import asyncio
import signal
import json
import logging
from dotenv import load_dotenv
from agents import Agent, Runner, RunHooks, ModelSettings

from tools.tools import (
    calculate_total_cost,
    calculate_tariff_for_countries,
    addtional_tariff_check
)

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

logging.basicConfig(level=logging.INFO)


class TariffRunContext:
    user_input: str
    session_id: str




class MyRunHooks(RunHooks):
    async def on_agent_start(self, ctx, agent):
        logging.info(f"\n [AGENT STARTED] {agent.name}")
        logging.info(f"Input Context: {ctx.context}")

    async def on_agent_end(self, ctx, agent, output):
        logging.info(f"\n[AGENT COMPLETED] {agent.name}")
        try:
            logging.info(f"Final Output:\n{json.dumps(output, indent=2)}")
        except Exception:
            logging.info(f" Final Output: {output}")

    async def on_tool_start(self, ctx, agent, tool):
        logging.info(f"\n [TOOL START] {tool.name}")
        logging.info(f" Tool Input Args:\n{json.dumps(ctx.tool_arguments, indent=2)}")

    async def on_tool_end(self, ctx, agent, tool, result):
        logging.info(f"[TOOL FINISHED] {tool.name}")
        try:
            logging.info(f"Tool Output:\n{json.dumps(result, indent=2)}")
        except Exception:
            logging.info(f"Tool Output: {result}")

    async def on_tool_error(self, ctx, agent, tool, error):
        logging.error(f"[TOOL ERROR] {tool.name}: {error}")
        
        
        

agent = Agent(
    name="TariffWorkflowAgent",
    instructions=(
        """
        SYSTEM PROMPT:
        You are a **Trade Duty & Tariff Intelligence Agent**.

        ---
        **Your Purpose:**
        Accurately calculate import tariffs, identify exclusions, and compute the final landed cost for a product.

        ---
        **Available Tools:**
        1️1- `calculate_tariff_for_countries()` → Calculate base tariff & program-based duty.  
        2️2- `addtional_tariff_check()` → Identify special tariffs, exclusions, or reciprocal duties from policy documents (e.g. Trump 2.0).  
        3️3- `calculate_total_cost()` → Compute final landed cost including MPF and HMF fees.  

        ---
        **Workflow (MUST follow strictly):**
        Step 1: Start with `calculate_tariff_for_countries()` using the provided HTS code, description, and country.  
        - Return the result as JSON under `"base_tariff"`.

        Step 2: Run `addtional_tariff_check()` using the same HTS + country + product description.  
        - Return any matching exclusions or special tariffs as `"additional_tariffs"`.

        Step 3: Call `calculate_total_cost()` to compute the total landed cost, combining base tariff, MPF, and HMF.  
        - Return as `"total_cost_breakdown"`.

        Step 4: Combine all three steps and output a **single JSON object**:
        ```json
        {
            "base_tariff": {},
            "additional_tariffs": {},
            "total_cost_breakdown": {},
            "potential_exclusion_codes": []
        }
        ```

        ---
        **Rules & Guardrails:**
        -  Validate input fields before using tools.
        -  Never invent tariff rates or exclusions; rely only on tool results.
        -  Never include or use any fields related to file, image_url, embedding, doc_source, or vector.
        -  Output must always be valid JSON (machine-readable).
        -  If a tool fails or returns None, log it and continue with the next one.
        """
    ),
    
    tools=[
        calculate_tariff_for_countries,
        addtional_tariff_check,
        calculate_total_cost
    ],
    model="gpt-4o",
    model_settings=ModelSettings(
        temperature=0.4,
        max_tokens=5000
    ),
)

# running = True

# def stop_running():
#     global running
#     print("Stopping the application...")
#     running = False

# async def background_task():
#     """Example background task."""
#     while running:
#         print("Background task running...")
#         await asyncio.sleep(5)

# async def main():
#     """Main entry point for running the agent."""
#     global running
#     task = asyncio.create_task(background_task())
#     try:
#         while running:
#             user_input = input("Enter your input (or press Ctrl+C to stop): ")
#             result = await Runner.run(agent, user_input, hooks=MyRunHooks())
#             print(f"Result: {result}")
#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#     finally:
#         task.cancel()  
#         await task

# if __name__ == "__main__":
#     signal.signal(signal.SIGINT, stop_running)
#     signal.signal(signal.SIGTERM, stop_running)
#     asyncio.run(main())