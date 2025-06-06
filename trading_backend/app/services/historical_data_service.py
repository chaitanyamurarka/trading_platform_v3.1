# chaitanyamurarka/trading_platform_v3.1/trading_platform_v3.1-fd71c9072644cabd20e39b57bf2d47b25107e752/trading_backend/app/services/historical_data_service.py
from .. import pyiqfeed as iq
from ..dtn_iq_client import get_iqfeed_history_conn, is_iqfeed_service_launched # Add is_iqfeed_service_launched

from sqlalchemy.orm import Session
from datetime import datetime, timedelta, date as datetime_date, timezone
from typing import List, Optional ,Dict# Ensure Optional is imported if not already
from app.core.numba_resampling_kernels import launch_resample_ohlc # Your Numba/CUDA launcher
from .. import schemas, models
from ..core.cache import get_cached_ohlc_data, set_cached_ohlc_data, build_ohlc_cache_key
from ..dtn_iq_client import get_iqfeed_history_conn # NEW IMPORT
import logging
import numpy as np
import pandas as pd
import json
from ..core.cache import redis_client,CACHE_EXPIRATION_SECONDS
from pydantic import TypeAdapter # Added for bulk Pydantic model creation
from fastapi import BackgroundTasks,HTTPException # Add this import
from sqlalchemy import text # Add for raw SQL if needed for other parts, though CRUD should handle most
import uuid

# import time # time module was imported but not used in the provided file, can be removed if not needed elsewhere
def map_interval_to_iqfeed_params(interval_val: str) -> Optional[Dict[str, any]]:
    interval_val_lower = interval_val.lower()
    if interval_val_lower.endswith('s') or interval_val_lower.endswith('m') or interval_val_lower.endswith('h'):
        # For ANY intraday request that needs to go to DTN, we fetch 1s base data
        return {"interval_type": "s", "interval_len": 1} 
    elif interval_val_lower == "1d":
        # Daily data can be fetched as daily from DTN
        return None # Or specific params if your DTN client needs them for daily
    logging.warning(f"Interval '{interval_val}' not mapped for DTN fetch params (or not intraday).")
    return None

def parse_iqfeed_bar_data(bar_data_item: np.void, interval_val: str) -> Optional[schemas.CandleBase]:
    """
    Parses a single row of IQFeed historical bar data (from numpy structured array)
    into a CandleBase schema.
    The dtype of bar_data_item depends on the request (daily vs intraday).
    """
    try:
        dt_only_date = bar_data_item['date'].astype(datetime_date) 
        
        if 'time' in bar_data_item.dtype.names: # Intraday bars
            time_val_timedelta64 = bar_data_item['time'] # This is numpy.timedelta64[us]
            
            # Convert numpy.timedelta64[us] to an integer number of microseconds
            microseconds_offset_np = time_val_timedelta64.astype(np.int64)
            # Explicitly convert numpy.int64 to Python int for timedelta
            microseconds_offset_py = int(microseconds_offset_np) 
            
            ts_value = datetime.combine(dt_only_date, datetime.min.time()) + timedelta(microseconds=microseconds_offset_py)
        else: # Daily bars
            ts_value = datetime.combine(dt_only_date, datetime.min.time())

        volume = 0
        if 'prd_vlm' in bar_data_item.dtype.names: 
            volume = float(bar_data_item['prd_vlm'])
        elif 'tot_vlm' in bar_data_item.dtype.names: 
            logging.warning(f"Using 'tot_vlm' as fallback for volume for {bar_data_item['date']} interval {interval_val}, ensure this is correct.")
            volume = float(bar_data_item['tot_vlm'])

        return schemas.CandleBase(
            timestamp=ts_value,
            open=float(bar_data_item['open_p']),
            high=float(bar_data_item['high_p']),
            low=float(bar_data_item['low_p']),
            close=float(bar_data_item['close_p']),
            volume=volume,
        )
    except (KeyError, ValueError, TypeError, AttributeError) as e:
        logging.error(f"Error parsing IQFeed bar item: {bar_data_item!r}. Error: {e}", exc_info=True)
        return None

# In app/services/historical_data_service.py
def fetch_from_dtn_iq_api(
    trading_symbol: str, 
    interval_val: str,
    start_time: datetime,
    end_time: datetime,
) -> List[schemas.CandleBase]:
    logging.info(f"Attempting to fetch from DTN IQFeed for {trading_symbol}, Interval: {interval_val}, Period: {start_time} to {end_time}")

    hist_conn = get_iqfeed_history_conn()
    if not hist_conn:
        logging.error("DTN IQFeed History Connection not available. Cannot fetch from IQFeed.")
        return []

    candles_from_iqfeed: List[schemas.CandleBase] = []
    
    try:
        with iq.ConnConnector([hist_conn]): 
            api_response_data = None
            if interval_val == "1d":
                logging.debug(f"Requesting daily data for {trading_symbol} from {start_time.date()} to {end_time.date()}")
                api_response_data = hist_conn.request_daily_data_for_dates(
                    ticker=trading_symbol,
                    bgn_dt=start_time.date(),
                    end_dt=end_time.date(),
                    ascend=True # Request sorted data
                )
            else:
                iq_interval_params = map_interval_to_iqfeed_params(interval_val)
                if not iq_interval_params:
                    logging.error(f"Could not map interval '{interval_val}' to IQFeed parameters for {trading_symbol}.")
                    return []

                logging.debug(f"Requesting intraday bars for {trading_symbol}, type: {iq_interval_params['interval_type']}, len: {iq_interval_params['interval_len']}, from {start_time} to {end_time}")
                api_response_data = hist_conn.request_bars_in_period(
                    ticker=trading_symbol,
                    interval_len=iq_interval_params["interval_len"],
                    interval_type=iq_interval_params["interval_type"],
                    bgn_prd=start_time, 
                    end_prd=end_time,   
                    ascend=True, # Request sorted data
                )
            
            if api_response_data is None or (isinstance(api_response_data, list) and not api_response_data):
                logging.info(f"No data or empty list returned from IQFeed for {trading_symbol} ({interval_val}).")
                return []
            elif isinstance(api_response_data, np.ndarray):
                if api_response_data.size == 0:
                    logging.info(f"Empty NumPy array returned from IQFeed for {trading_symbol} ({interval_val}).")
                    return []

                logging.info(f"Received {len(api_response_data)} records from IQFeed for {trading_symbol} ({interval_val}). Processing with optimized parsing...")

                if 'time' in api_response_data.dtype.names:
                    timestamps_dt64 = api_response_data['date'] + api_response_data['time']
                else:
                    timestamps_dt64 = api_response_data['date']

                start_time_naive = start_time.replace(tzinfo=None)
                end_time_naive = end_time.replace(tzinfo=None)
                
                start_time_np = np.datetime64(start_time_naive)
                end_time_np = np.datetime64(end_time_naive)

                if timestamps_dt64.dtype == np.dtype('datetime64[D]'):
                    start_time_np = start_time_np.astype('datetime64[D]')
                    end_time_np = end_time_np.astype('datetime64[D]')
                
                mask = (timestamps_dt64 >= start_time_np) & (timestamps_dt64 <= end_time_np)
                filtered_data = api_response_data[mask]
                
                if filtered_data.size == 0:
                    logging.info(f"No data remains for {trading_symbol} after time filtering ({start_time} to {end_time}).")
                    return []

                filtered_timestamps_dt64 = timestamps_dt64[mask]

                python_timestamps = pd.to_datetime(filtered_timestamps_dt64, utc=True).to_pydatetime()

                open_prices = filtered_data['open_p'].astype(float)
                high_prices = filtered_data['high_p'].astype(float)
                low_prices = filtered_data['low_p'].astype(float)
                close_prices = filtered_data['close_p'].astype(float)

                if 'prd_vlm' in filtered_data.dtype.names:
                    volumes = filtered_data['prd_vlm'].astype(float)
                elif 'tot_vlm' in filtered_data.dtype.names:
                    volumes = filtered_data['tot_vlm'].astype(float)
                    logging.warning(f"Using 'tot_vlm' as fallback for volume for {trading_symbol}, interval {interval_val}.")
                else:
                    volumes = np.zeros(filtered_data.size, dtype=float)

                list_of_dicts = []
                for i in range(filtered_data.size):
                    list_of_dicts.append({
                        "timestamp": python_timestamps[i],
                        "open": open_prices[i],
                        "high": high_prices[i],
                        "low": low_prices[i],
                        "close": close_prices[i],
                        "volume": volumes[i]
                    })

                if list_of_dicts:
                    candle_adapter = TypeAdapter(List[schemas.CandleBase])
                    candles_from_iqfeed = candle_adapter.validate_python(list_of_dicts)
                else:
                    candles_from_iqfeed = []
                
                logging.info(f"Optimized parsing complete, {len(candles_from_iqfeed)} candles mapped for {trading_symbol} ({interval_val}).")

            else:
                logging.warning(f"Unexpected data type from IQFeed: {type(api_response_data)} for {trading_symbol}")
                return []

    except iq.NoDataError:
        logging.info(f"IQFeed: NoDataError for {trading_symbol} interval {interval_val} from {start_time} to {end_time}.")
    except iq.UnauthorizedError as e:
        logging.error(f"IQFeed: UnauthorizedError for {trading_symbol}: {e}")
    except Exception as e:
        logging.error(
            f"Exception during IQFeed API call/processing for {trading_symbol}: {e}",
            exc_info=True,
        )
    
    return candles_from_iqfeed

INTERVAL_SECONDS_MAP = {
    "1s": 1, "5s": 5, "10s": 10, "15s": 15, "30s": 30, "45s": 45,
    "1m": 60, "5m": 300, "10m": 600, "15m": 900,
    "30m": 1800, "45m": 2700, "1h": 3600, "1d": 86400
}

def _get_and_prepare_1s_data_for_range(
    background_tasks: BackgroundTasks,
    session_token: str,
    exchange: str,
    token: str,
    start_time: datetime,
    end_time: datetime
) -> List[schemas.CandleBase]:
    all_1s_candles = []
    date_range = pd.date_range(start_time.date(), end_time.date())
    
    cache_keys_to_check = [
        build_ohlc_cache_key(exchange, token, "1s", day.strftime('%Y-%m-%d'), session_token=session_token)
        for day in date_range
    ]
    
    logging.info(f"Performing parallel cache check for {len(cache_keys_to_check)} keys.")
    cached_results = redis_client.mget(cache_keys_to_check)
    
    missing_dates = []
    for i, result in enumerate(cached_results):
        day = date_range[i].date()
        if result:
            logging.debug(f"Cache hit for 1s data on {day}")
            try:
                deserialized = json.loads(result)
                if deserialized: 
                    all_1s_candles.extend([schemas.CandleBase(**item) for item in deserialized])
            except (json.JSONDecodeError, TypeError):
                logging.warning(f"Could not parse cached 1s data for {day}. Refetching.")
                missing_dates.append(day)
        else:
            logging.debug(f"Cache miss for 1s data on {day}")
            missing_dates.append(day)

    if missing_dates:
        fetch_start_date = min(missing_dates)
        fetch_end_date = max(missing_dates)
        
        fetch_start_time = datetime.combine(fetch_start_date, datetime.min.time())
        fetch_end_time = datetime.combine(fetch_end_date, datetime.max.time())

        logging.info(f"Fetching missing 1s data from DTN for {len(missing_dates)} dates in range: {fetch_start_time} to {fetch_end_time}")
        
        newly_fetched_data = fetch_from_dtn_iq_api(
            trading_symbol=token,
            interval_val="1s",
            start_time=fetch_start_time,
            end_time=fetch_end_time
        )
        
        if newly_fetched_data:
            all_1s_candles.extend(newly_fetched_data)
            new_data_df = pd.DataFrame([c.model_dump() for c in newly_fetched_data])
            if not new_data_df.empty:
                new_data_df['timestamp'] = pd.to_datetime(new_data_df['timestamp'])
                new_data_df['date_key'] = new_data_df['timestamp'].dt.strftime('%Y-%m-%d')
                grouped_new_data = {date_key: group for date_key, group in new_data_df.groupby('date_key')}
                
                logging.info(f"Queueing {len(missing_dates)} daily records into Redis pipeline for caching.")
                pipe = redis_client.pipeline()
                for day in missing_dates:
                    date_str = day.strftime('%Y-%m-%d')
                    day_cache_key = build_ohlc_cache_key(exchange, token, "1s", date_str, session_token=session_token)
                    
                    if date_str in grouped_new_data:
                        group = grouped_new_data[date_str]
                        # =================== FIX START ===================
                        # Convert the DataFrame group to a list of dictionaries first
                        records_to_cache = group.to_dict(orient='records')
                        # Now, iterate through the list and convert the Timestamp object to a string
                        for record in records_to_cache:
                            record['timestamp'] = record['timestamp'].isoformat()
                        # Now the list of dictionaries is fully JSON serializable
                        pipe.set(day_cache_key, json.dumps(records_to_cache), ex=CACHE_EXPIRATION_SECONDS)
                        # =================== FIX END ===================
                    else:
                        pipe.set(day_cache_key, json.dumps([]), ex=CACHE_EXPIRATION_SECONDS)
                
                pipe.execute()
                logging.info("Redis pipeline execution complete.")

    all_1s_candles.sort(key=lambda c: c.timestamp)
    start_time_aware = start_time.replace(tzinfo=timezone.utc) if start_time.tzinfo is None else start_time
    end_time_aware = end_time.replace(tzinfo=timezone.utc) if end_time.tzinfo is None else end_time
    
    final_filtered_candles = [
        c for c in all_1s_candles if start_time_aware <= c.timestamp <= end_time_aware
    ]

    return final_filtered_candles

def _process_and_cache_full_data(
    background_tasks: BackgroundTasks,
    session_token: str,
    exchange: str,
    token: str,
    interval_val: str,
    start_time: datetime,
    end_time: datetime
) -> Optional[str]:
    """
    Fetches, processes, and caches the ENTIRE dataset for a given range.
    Returns the cache key (request_id) where the full data is stored.
    """
    base_1s_candles = _get_and_prepare_1s_data_for_range(
        background_tasks, session_token, exchange, token, start_time, end_time
    )

    if not base_1s_candles:
        logging.warning(f"No 1s base data found for {exchange}:{token} in range to process.")
        return None

    final_candles: List[schemas.Candle]
    if interval_val == "1s":
        final_candles = [schemas.Candle(**c.model_dump()) for c in base_1s_candles]
    else:
        aggregation_seconds = INTERVAL_SECONDS_MAP.get(interval_val)
        if not aggregation_seconds:
            raise HTTPException(status_code=400, detail=f"Unsupported interval for resampling: {interval_val}")

        # Prepare numpy arrays for Numba kernel
        timestamps_1s_np = np.array([c.timestamp.replace(tzinfo=timezone.utc).timestamp() for c in base_1s_candles], dtype=np.float64)
        open_1s_np = np.array([c.open for c in base_1s_candles], dtype=np.float64)
        high_1s_np = np.array([c.high for c in base_1s_candles], dtype=np.float64)
        low_1s_np = np.array([c.low for c in base_1s_candles], dtype=np.float64)
        close_1s_np = np.array([c.close for c in base_1s_candles], dtype=np.float64)
        volume_1s_np = np.array([c.volume if c.volume is not None else 0.0 for c in base_1s_candles], dtype=np.float64)
        
        # Launch resampling
        (ts_agg, o_agg, h_agg, l_agg, c_agg, v_agg, num_agg_bars) = launch_resample_ohlc(
            timestamps_1s_np, open_1s_np, high_1s_np, low_1s_np, close_1s_np, volume_1s_np,
            aggregation_seconds
        )

        resampled_candles: List[schemas.Candle] = []
        for i in range(num_agg_bars):
            agg_dt = datetime.fromtimestamp(ts_agg[i], tz=timezone.utc)
            resampled_candles.append(schemas.Candle(
                timestamp=agg_dt,
                open=o_agg[i], high=h_agg[i], low=l_agg[i], close=c_agg[i], volume=v_agg[i]
            ))
        final_candles = resampled_candles

    if final_candles:
        request_id = f"chart_data:{session_token}:{uuid.uuid4()}"
        set_cached_ohlc_data(request_id, final_candles, expiration=3600)  # Cache for 1 hour
        logging.info(f"Full dataset with {len(final_candles)} candles processed and cached with request_id: {request_id}")
        return request_id
    
    return None

def get_initial_historical_data(
    background_tasks: BackgroundTasks,
    session_token: str,
    exchange: str,
    token: str,
    interval_val: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 5000
) -> schemas.HistoricalDataResponse:
    
    request_id = _process_and_cache_full_data(
        background_tasks, session_token, exchange, token, interval_val, start_time, end_time
    )

    if not request_id:
        return schemas.HistoricalDataResponse(candles=[], total_available=0, is_partial=False, message="No data available for the selected range.", request_id=None, offset=None)

    full_data = get_cached_ohlc_data(request_id)
    if not full_data:
        return schemas.HistoricalDataResponse(candles=[], total_available=0, is_partial=False, message="Error retrieving processed data from cache.", request_id=request_id, offset=None)
        
    total_available = len(full_data)
    initial_offset = max(0, total_available - limit)
    
    candles_to_send = full_data[initial_offset:] 
    
    return schemas.HistoricalDataResponse(
        request_id=request_id,
        candles=candles_to_send,
        offset=initial_offset,
        total_available=total_available,
        is_partial=total_available > len(candles_to_send),
        message=f"Initial data loaded. Displaying last {len(candles_to_send)} of {total_available} candles."
    )

def get_historical_data_chunk(
    request_id: str,
    offset: int,
    limit: int = 5000
) -> schemas.HistoricalDataChunkResponse:
    
    if not request_id.startswith("chart_data:"):
        raise HTTPException(status_code=400, detail="Invalid request_id format.")

    full_data = get_cached_ohlc_data(request_id)

    if full_data is None:
        raise HTTPException(status_code=404, detail="Data for this request not found or has expired.")

    total_available = len(full_data)
    
    if offset >= total_available:
        return schemas.HistoricalDataChunkResponse(candles=[], offset=offset, limit=limit, total_available=total_available)
        
    chunk = full_data[offset : offset + limit]
    
    return schemas.HistoricalDataChunkResponse(
        candles=chunk,
        offset=offset,
        limit=limit,
        total_available=total_available
    )