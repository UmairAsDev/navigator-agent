import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from agents import Agent, Runner
from utils.utils import _set_env



_set_env()

agent = Agent(name="Assistant", instructions="You are a helpful assistant")

result = Runner.run_sync(agent, "Write a haiku about recursion in programming.")
print(result.final_output)

# Code within the code,
# Functions calling themselves,
# Infinite loop's dance.






# {
#   "user_input": "string",
#   "payload": [
#     {
#       "hts_code": "0102.29.40.62",
#       "description": "Live bovine animals:",
#       "spec_levels": [
#         "Other",
#         "Weighing 320 kg or more each:",
#         "For immediate slaughter:",
#         "Steers",
#         "Cattle:",
#         "Other:"
#       ],
#       "specific_rate_of_duty": "nan",
#       "column2_rate_of_duty": "nan",
#       "general_rate_of_duty": "nan",
#       "text": "prefix4:0102 | prefix6:010229 | Cattle: | Other: | Other | Weighing 320 kg or more each: | For immediate slaughter: | Steers"
#     }
#   ],
#   "hts_code": "string",
#   "base_cost": 0,
#   "country": "string",
#   "mode_of_transport": [
#     "string"
#   ],
#   "entry_date": "2025-10-23",
#   "date_of_loading": "2025-10-23",
#   "potential_exclusion_code": "string"
# }