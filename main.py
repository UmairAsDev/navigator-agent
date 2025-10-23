import os
import sys
from fastapi import FastAPI
import uvicorn


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from controller.tariff_routes import router


app = FastAPI()
app.include_router(router)

if __name__ == "__main__":

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")