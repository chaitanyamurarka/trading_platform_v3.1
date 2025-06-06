from sqlalchemy import UniqueConstraint
from typing import List, Optional, Dict, Any, Literal, Union # Added Union
from pydantic import BaseModel, Field, validator, field_validator

# Suggested update (ensure imports are correct):
from sqlalchemy import Column, String, DateTime, Float, Integer, Index, UniqueConstraint
# from .database import Base # Assuming Base is in database.py; it's defined in your file for now.
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class OHLC(Base):
    __tablename__ = "ohlc_data"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    exchange = Column(String(50), nullable=False)
    token = Column(String(50), nullable=False)
    interval = Column(String(10), nullable=False) # e.g., '1s', '5m', '1h', '1d'
    timestamp = Column(DateTime, nullable=False)

    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)

    # This composite unique constraint is essential for INSERT ... ON DUPLICATE KEY UPDATE.
    # This index will also serve most query needs.
    __table_args__ = (
        UniqueConstraint("exchange", "token", "interval", "timestamp", name="uq_ohlc_exchange_token_interval_timestamp"),
        Index("idx_ohlc_query_all_filters", "exchange", "token", "interval", "timestamp") # Covers all main query filters
    )

    def __repr__(self):
        return (f"<OHLC(exchange='{self.exchange}', token='{self.token}', interval='{self.interval}', "
                f"timestamp='{self.timestamp}', open={self.open}, high={self.high}, "
                f"low={self.low}, close={self.close}, volume={self.volume})>")

class StrategyParameter(BaseModel):
    """Defines a parameter for a trading strategy."""
    name: str = Field(description="Internal name of the parameter.")
    label: Optional[str] = Field(None, description="User-friendly display name for the parameter.")
    type: Literal['int', 'float', 'bool', 'choice'] = Field(description="Data type of the parameter.")
    default: Any = Field(description="Default value for the parameter.")
    value: Optional[Any] = Field(None, description="Actual value to be used (set during backtest/optimization).")
    min_value: Optional[float] = Field(None, description="Minimum allowed value (for numeric types).")
    max_value: Optional[float] = Field(None, description="Maximum allowed value (for numeric types).")
    step: Optional[float] = Field(None, description="Step for numeric ranges (e.g., for optimizers).")
    choices: Optional[List[Any]] = Field(None, description="List of allowed values (for 'choice' type).")
    description: Optional[str] = Field(None, description="Explanation of the parameter.")
    # Proposed additions for UI/UX enhancements
    min_opt_range: Optional[float] = Field(None, description="Default minimum value for optimization range if applicable.")
    max_opt_range: Optional[float] = Field(None, description="Default maximum value for optimization range if applicable.")
    step_opt_range: Optional[float] = Field(None, description="Default step for optimization range if applicable.")
    category: Optional[str] = Field(None, description="UI Category (e.g., 'Entry Logic', 'Risk Management', 'General').")
    

class StrategyInfo(BaseModel):
    """Provides metadata about an available trading strategy."""
    id: str = Field(description="Unique identifier for the strategy.")
    name: str = Field(description="User-friendly name of the strategy.")
    description: Optional[str] = Field(None)
    parameters: List[StrategyParameter] = Field(description="List of parameters the strategy accepts.")

class IndicatorDataPoint(BaseModel):
    time: int # UNIX timestamp in seconds
    value: Optional[float] = None # Allow for NaN or gaps in indicator data

class IndicatorConfig(BaseModel):
    color: str = "blue"
    lineWidth: int = 1
    paneId: str = "main_chart" # e.g., "main_chart", "rsi_pane"
    priceScaleId: Optional[str] = None # e.g., "rsi_price_scale" for separate y-axis

class IndicatorSeries(BaseModel):
    name: str # For display/legend, e.g., "Fast EMA (10)"
    data: List[IndicatorDataPoint]
    config: IndicatorConfig
