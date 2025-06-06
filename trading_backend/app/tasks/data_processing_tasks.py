# trading_backend/app/tasks/data_processing_tasks.py

from .celery_app import celery_application
from app.core.cache import get_cached_ohlc_data, set_cached_ohlc_data
from app.core.numba_resampling_kernels import launch_resample_ohlc
from app import schemas
import numpy as np
import json
from datetime import datetime, timezone
import logging

INTERVAL_SECONDS_MAP = {
    "1s": 1, "5s": 5, "10s": 10, "15s": 15, "30s": 30, "45s": 45,
    "1m": 60, "5m": 300, "10m": 600, "15m": 900,
    "30m": 1800, "45m": 2700, "1h": 3600, "1d": 86400
}

@celery_application.task(name="tasks.resample_and_cache_all_intervals")
def resample_and_cache_all_intervals_task(
    base_1s_data_key: str,
    request_id_prefix: str, # e.g., "chart_data_full:SESSION_TOKEN:EXCHANGE:TOKEN:START:END"
    user_requested_interval: str
):
    """
    Given a cache key for full-range 1s data, this task resamples it to all
    other standard intervals and caches each result under a specific key.
    """
    logging.info(f"Starting background resampling for prefix {request_id_prefix}")
    
    base_1s_candles = get_cached_ohlc_data(base_1s_data_key)
    if not base_1s_candles:
        logging.warning(f"No 1s base data at key {base_1s_data_key} to perform background resampling.")
        return
        
    # Prepare numpy arrays from the 1s data
    timestamps_1s_np = np.array([c.timestamp.replace(tzinfo=timezone.utc).timestamp() for c in base_1s_candles], dtype=np.float64)
    open_1s_np = np.array([c.open for c in base_1s_candles], dtype=np.float64)
    high_1s_np = np.array([c.high for c in base_1s_candles], dtype=np.float64)
    low_1s_np = np.array([c.low for c in base_1s_candles], dtype=np.float64)
    close_1s_np = np.array([c.close for c in base_1s_candles], dtype=np.float64)
    volume_1s_np = np.array([c.volume if c.volume is not None else 0.0 for c in base_1s_candles], dtype=np.float64)


    for interval, agg_seconds in INTERVAL_SECONDS_MAP.items():
        if interval == user_requested_interval or interval == "1s":
            continue # Skip the one the user already got and the base data

        logging.debug(f"Background resampling to {interval} for {request_id_prefix}")
        
        # Resample
        (ts_agg, o_agg, h_agg, l_agg, c_agg, v_agg, num_agg_bars) = launch_resample_ohlc(
            timestamps_1s_np, open_1s_np, high_1s_np, low_1s_np, close_1s_np, volume_1s_np,
            agg_seconds
        )
        
        if num_agg_bars > 0:
            resampled_candles = []
            for i in range(num_agg_bars):
                agg_dt = datetime.fromtimestamp(ts_agg[i], tz=timezone.utc)
                resampled_candles.append(schemas.Candle(
                    timestamp=agg_dt,
                    open=o_agg[i], high=h_agg[i], low=l_agg[i], close=c_agg[i], volume=v_agg[i]
                ))
            
            # Cache the result
            target_cache_key = f"{request_id_prefix}:{interval}"
            set_cached_ohlc_data(target_cache_key, resampled_candles, expiration=3600) # Cache for 1 hour
    
    logging.info(f"Finished background resampling for {request_id_prefix}")