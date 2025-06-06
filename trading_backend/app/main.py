import logging
import sys # Import sys for directing print to stdout for logging
import os ## <<< ADD THIS IMPORT


# --- Add this basic logging configuration ---
logging.basicConfig(
    level=logging.INFO, # Set to DEBUG to capture everything
    format="%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - line %(lineno)d - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout) # Ensure logs go to stdout
    ]
)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse ## <<< ADD THIS IMPORT
from fastapi.staticfiles import StaticFiles ## <<< ADD THIS IMPORT

# Import your database components and models
from . import models # This ensures models are registered with Base
from .dtn_iq_client import launch_iqfeed_service_if_needed # NEW IMPORT

from .core import strategy_loader

from typing import List

from app.services.live_data_feed_service import live_feed_service
from app.config import settings # To check if DTN is configured

from .models import StrategyInfo



app = FastAPI(
    title="Trading Platform API",
    description="Backend API for historical data and strategy optimization tasks.",
    version="0.0.1"
)

# --- Determine the correct path to the frontend directory ---
# This assumes 'main.py' is in 'trading_backend/app/'
# and 'frontend' is a sibling to 'trading_backend'
# So, ../../frontend
script_dir = os.path.dirname(__file__) # trading_backend/app
backend_root_dir = os.path.dirname(script_dir) # trading_backend
project_root_dir = os.path.dirname(backend_root_dir) # trading_platform_v3-ae0e...
frontend_dir = os.path.join(project_root_dir, "frontend")
static_dir = os.path.join(frontend_dir, "static")

# --- Mount static files directory ---
# This will serve files from 'frontend/static' under the path '/static'
# e.g., a request to '/static/css/style.css' will serve 'frontend/static/css/style.css'
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    logging.info(f"Mounted static directory: {static_dir}")
else:
    logging.error(f"Static directory not found at: {static_dir}. Static files will not be served.")


app.add_middleware(GZipMiddleware, minimum_size=1000) # Compress if > 1KB

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SYMBOLS_TO_SUBSCRIBE_LIVE = ["AAPL", "AMZA", "TSLA", "@NQ#"]

@app.on_event("startup")
async def startup_event():
    logging.info("Application starting up...")
    logging.info("Database tables checked/created.")

    # Launch IQFeed (already in your main.py)
    launch_iqfeed_service_if_needed() # This is from app.dtn_iq_client
    logging.info("Application startup complete.")

@app.get("/")
async def root():
    index_html_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_html_path):
        with open(index_html_path, "r") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=200)
    else:
        logging.error(f"index.html not found at: {index_html_path}")
        raise HTTPException(status_code=404, detail="index.html not found")

# Example endpoint to list available strategies
@app.get("/strategies", response_model=List[StrategyInfo]) # Assuming StrategyInfo is your Pydantic model
async def list_available_strategies():
    return strategy_loader.get_available_strategies_info()

# Example endpoint to get info for a specific strategy
@app.get("/strategies/{strategy_id}", response_model=StrategyInfo)
async def get_strategy_details(strategy_id: str):
    strategy_class = strategy_loader.get_strategy_class(strategy_id)
    if not strategy_class:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy_class.get_info()

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Application shutting down...")
    live_feed_service.disconnect()
    logging.info("Live feed service disconnected.")
    # Add other shutdown logic here if needed

# Import and include your routers
from .routers import historical_data_router, utility_router

app.include_router(historical_data_router.router)
# app.include_router(optimization_router.router)
app.include_router(utility_router.router)