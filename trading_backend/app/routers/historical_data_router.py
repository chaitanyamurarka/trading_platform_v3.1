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

# MODIFIED: The response_model is now schemas.HistoricalDataResponse
@router.get("/", response_model=schemas.HistoricalDataResponse)
async def fetch_historical_data(
    background_tasks: BackgroundTasks,
    session_token: str = Query(..., description="The user's session token."),
    exchange: str = Query(..., description="Exchange name or code (e.g., 'NASDAQ')"),
    token: str = Query(..., description="Asset symbol or token (e.g., 'AAPL')"),
    interval: schemas.Interval = Query(..., description="Data interval (e.g., '1m', '5m', '1d')"),
    start_time: datetime = Query(..., description="Start datetime for the data range (ISO format, e.g., '2023-01-01T00:00:00')"),
    end_time: datetime = Query(..., description="End datetime for the data range (ISO format, e.g., '2023-01-01T12:00:00')"),
):
    """
    Retrieve historical OHLC candlestick data.
    The server fetches all available data for the range, then sends it back in a structured
    response, capping the initial payload at 5000 candles if the total is larger.
    """
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

    # Call the service layer function, which will now return the new structured response
    historical_data_response = historical_data_service.get_historical_data_with_fetch(
        background_tasks=background_tasks,
        session_token=session_token,
        exchange=exchange,
        token=token,
        interval_val=interval.value,
        start_time=start_time,
        end_time=end_time
    )

    return historical_data_response