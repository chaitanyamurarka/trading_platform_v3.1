# chaitanyamurarka/trading_platform_v3.1/trading_platform_v3.1-fd71c9072644cabd20e39b57bf2d47b25107e752/trading_backend/app/schemas.py
from pydantic import BaseModel, Field, model_validator 
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

class Interval(str, Enum):
    """Allowed timeframe intervals for OHLC data."""
    SEC_1 = "1s"
    SEC_5 = "5s"
    SEC_10 = "10s"
    SEC_15 = "15s"
    SEC_30 = "30s"
    SEC_45 = "45s"
    MIN_1 = "1m"
    MIN_5 = "5m"
    MIN_10 = "10m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    MIN_45 = "45m"
    HOUR_1 = "1h"
    DAY_1 = "1d"
    # Add more intervals as needed, e.g., '1w', '1mo'

class CandleBase(BaseModel):
    """Base schema for OHLC data point."""
    timestamp: datetime # Pydantic v2 uses 'timestamp: datetime' directly
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    unix_timestamp: Optional[float] = None # New field for UNIX timestamp

    @model_validator(mode='after')
    def calculate_unix_timestamp(self) -> 'CandleBase':
        """Calculates UNIX timestamp from the datetime timestamp."""
        if self.timestamp:
            self.unix_timestamp = self.timestamp.timestamp()
        return self

class Candle(CandleBase):
    """Schema for a single OHLC data point, including ORM mode."""
    class Config:
        from_attributes = True # For Pydantic v2 (replaces orm_mode)

class HistoricalQuery(BaseModel): # For request query parameters, though FastAPI can infer from function args
    """Schema for historical data query parameters."""
    exchange: str
    token: str
    interval: Interval
    start_time: datetime # Renamed from 'start' to avoid conflict with potential keywords and be more descriptive
    end_time: datetime   # Renamed from 'end'

class OptimizationRequest(BaseModel):
    """Schema for optimization task submission."""
    strategy_id: str # Changed to str to be more flexible than int
    symbol: str
    interval: Interval # Added interval for consistency
    start_date: datetime
    end_date: datetime
    param_grid: Dict[str, List[Any]] # Example: {"param1": [10, 20], "param2": [True, False]}

class OptimizationTaskResult(BaseModel):
    """Schema for the result of an optimization task."""
    best_params: Dict[str, Any]
    best_score: float
    # You might want to add more details like all results, not just the best
    # all_results: Optional[List[Dict[str, Any]]] = None

class JobStatus(str, Enum):
    PENDING = "PENDING"
    RECEIVED = "RECEIVED" # Celery's PENDING often means it's in the queue
    STARTED = "STARTED"   # Task execution has begun
    SUCCESS = "SUCCESS"   # Task completed successfully
    FAILURE = "FAILURE"   # Task failed
    RETRY = "RETRY"       # Task is being retried
    REVOKED = "REVOKED"   # Task was cancelled/revoked

class JobStatusResponse(BaseModel):
    """Schema for job status query response."""
    job_id: str # Renamed from task_id for clarity if needed
    status: JobStatus # Using the Enum for status
    # message: Optional[str] = None # Optional field for more details/errors
    result: Optional[OptimizationTaskResult] = None #
    # progress: Optional[float] = None # e.g., 0.0 to 1.0

# Schema for submitting a job, usually just returns a job ID
class JobSubmissionResponse(BaseModel):
    job_id: str
    status: JobStatus = JobStatus.RECEIVED # Initial status
    message: Optional[str] = "Job submitted successfully."

class HistoricalDataResponse(BaseModel):
    """
    Defines the structured response for historical data requests.
    Includes the candle data plus metadata about the load.
    """
    request_id: Optional[str] = Field(None, description="A unique ID for this data request, used for fetching subsequent chunks.")
    candles: List[Candle] = Field(description="The list of OHLC candle data.")
    offset: Optional[int] = Field(None, description="The starting offset of this chunk within the full dataset.")
    total_available: int = Field(description="The total number of candles available on the server for the requested range.")
    is_partial: bool = Field(description="True if the returned 'candles' are a subset of the total available (due to size limit).")
    message: str = Field(description="A message describing the result of the data load.")

class HistoricalDataChunkResponse(BaseModel):
    candles: List[Candle]
    offset: int
    limit: int
    total_available: int

class SessionInfo(BaseModel):
    session_token: str