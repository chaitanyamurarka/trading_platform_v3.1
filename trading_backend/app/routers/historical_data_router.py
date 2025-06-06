# app/routers/historical_data_router.py

from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from datetime import datetime
from typing import List, Optional

from .. import schemas
# from ..crud import crud_historical_data # No longer directly used by this endpoint
from ..services import historical_data_service # Import the service

router = APIRouter(
    prefix="/historical",
    tags=["Historical Data"]
)

@router.get("/", response_model=List[schemas.Candle])
async def fetch_historical_data(
    background_tasks: BackgroundTasks, # FastAPI will inject this
    session_token: str = Query(..., description="The user's session token."),
    exchange: str = Query(..., description="Exchange name or code (e.g., 'NSE', 'BSE')"),
    token: str = Query(..., description="Asset symbol or token (e.g., 'RELIANCE', 'SBIN')"),
    interval: schemas.Interval = Query(..., description="Data interval (e.g., '1m', '5m', '1d')"),
    start_time: datetime = Query(..., description="Start datetime for the data range (ISO format, e.g., '2023-01-01T00:00:00')"),
    end_time: datetime = Query(..., description="End datetime for the data range (ISO format, e.g., '2023-01-01T12:00:00')"),
):
    """
    Retrieve historical OHLC candlestick data for a given exchange, token, interval, and time range.
    Data is sourced from cache, then database, then external API if necessary.
    Database writes and caching for new data are performed in the background.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    # Call the service layer function, now passing background_tasks and session_token
    ohlc_data = historical_data_service.get_historical_data_with_fetch(
        background_tasks=background_tasks,
        session_token=session_token,
        exchange=exchange,
        token=token,
        interval_val=interval.value,
        start_time=start_time,
        end_time=end_time
    )

    return ohlc_data