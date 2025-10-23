import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".../..")))
import asyncio
import signal
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
        logging.info(f"[HOOK] Agent {agent.name} started with input: {ctx.context}")
        
    async def on_agent_end(self, ctx, agent, output):
        logging.info(f"[HOOK] Agent {agent.name} ended with output: {output}")
        
    async def on_tool_start(self, ctx, agent, tool):
        logging.info(f"[HOOK] Tool {tool.name} about to run with args: {ctx.tool_arguments}")
        
    async def on_tool_end(self, ctx, agent, tool, result):
        logging.info(f"[HOOK] Tool {tool.name} finished with result: {result}")


agent = Agent(
    name="TariffWorkflowAgent",
    instructions=(
        """
        SYSTEM PROMPT:
        You are a Trade Duty & Tariff Intelligence Agent.

        Available Tools:
        1. calculate_tariff_for_countries() – Calculate base tariff & program-based duty.
        2. calculate_total_cost() – Compute total landed cost (with MPF/HMF fees).
        3. addtional_tariff_check() – Identify special tariffs, exclusions, or reciprocal duties from policy docs.

        Workflow:
        - Step 1: User provides HTS payload, country, transport mode, and dates.
        - Step 2: Use calculate_tariff_for_countries() for base tariff.
        - Step 3: Use addtional_tariff_check() for Trump 2.0 and other policy-based adjustments.
        - Step 4: Combine all and compute final cost using calculate_total_cost().
        - Step 5: Return a clear, structured tariff breakdown.
        Gaurdrails:
        - Always validate inputs before calculations.
        - Handle exceptions gracefully, providing informative error messages.
        - Ensure compliance with the latest trade regulations and policies.
        - Prioritize accuracy and clarity in all responses.
        - Do not make up rates or fees; always use tool outputs.
        - Do not include or use any variable regarding file processing image_url, file_id, thumbnail, embedding, vector, doc_source.
        """
    ),
    tools=[
        calculate_tariff_for_countries,
        calculate_total_cost,
        addtional_tariff_check
    ],
    model="gpt-4o",
    model_settings=ModelSettings(temperature=0.7, max_tokens=3000),
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