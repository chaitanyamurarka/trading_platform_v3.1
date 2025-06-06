# In app/tasks/aggregation_tasks.py
from .celery_app import celery_application
from app.database import SessionLocal
from app.crud import crud_historical_data
from app import schemas # For creating CandleBase objects
from app.core.numba_resampling_kernels import launch_resample_ohlc # Assuming you create this
from datetime import datetime, timezone
import numpy as np
import logging
import pandas as pd # For easier conversion from ORM to NumPy if needed
from typing import List

TARGET_AGGREGATE_INTERVALS = ["5s", "1m", "5m", "15m", "1h", "1d"] 
INTERVAL_SECONDS_MAP = { # Moved here or to a config
    "5s": 5, "10s": 10, "15s": 15, "30s": 30, "45s": 45,
    "1m": 60, "5m": 300, "10m": 600, "15m": 900,
    "30m": 1800, "45m": 2700, "1h": 3600, "1d": 86400
}

@celery_application.task(name="tasks.generate_aggregates_for_range")
def generate_aggregates_for_range_task(exchange: str, token: str, start_time_iso: str, end_time_iso: str):
    db = SessionLocal()
    try:
        start_time_dt = datetime.fromisoformat(start_time_iso)
        end_time_dt = datetime.fromisoformat(end_time_iso)
        
        logging.info(f"Celery task: Starting Numba/CUDA aggregation for {exchange}:{token} from {start_time_dt} to {end_time_dt}")

        # 1. Fetch 1s base data from DB for the entire range
        # Ensure this fetches data ordered by timestamp
        base_1s_records_orm = crud_historical_data.get_ohlc_data(
            db, exchange, token, "1s", start_time_dt, end_time_dt
        )

        if not base_1s_records_orm:
            logging.info(f"No 1s base data found for {exchange}:{token} in range {start_time_dt}-{end_time_dt}. Skipping aggregation.")
            return

        logging.info(f"Fetched {len(base_1s_records_orm)} 1s records from DB for Numba processing.")

        # 2. Convert ORM objects to NumPy arrays (host memory)
        #    Make sure timestamps are Unix timestamps (float or int) for easier Numba processing.
        #    Ensure consistent dtype, e.g., float64 for OHLCV.
        timestamps_1s_np = np.array([r.timestamp.replace(tzinfo=timezone.utc).timestamp() for r in base_1s_records_orm], dtype=np.float64)
        open_1s_np = np.array([r.open for r in base_1s_records_orm], dtype=np.float64)
        high_1s_np = np.array([r.high for r in base_1s_records_orm], dtype=np.float64)
        low_1s_np = np.array([r.low for r in base_1s_records_orm], dtype=np.float64)
        close_1s_np = np.array([r.close for r in base_1s_records_orm], dtype=np.float64)
        volume_1s_np = np.array([r.volume if r.volume is not None else 0.0 for r in base_1s_records_orm], dtype=np.float64)
        
        for target_interval in TARGET_AGGREGATE_INTERVALS:
            if target_interval not in INTERVAL_SECONDS_MAP:
                logging.warning(f"Unsupported target interval '{target_interval}'. Skipping.")
                continue
            
            aggregation_seconds = INTERVAL_SECONDS_MAP[target_interval]
            logging.info(f"Aggregating to {target_interval} ({aggregation_seconds}s) for {exchange}:{token} using Numba/CUDA...")

            # 3. Call the Numba/CUDA resampling launcher
            (ts_agg, o_agg, h_agg, l_agg, c_agg, v_agg, num_agg_bars) = launch_resample_ohlc(
                timestamps_1s_np, open_1s_np, high_1s_np, low_1s_np, close_1s_np, volume_1s_np,
                aggregation_seconds
            )

            if num_agg_bars > 0:
                # 4. Convert results back to list of schemas.CandleBase for storage
                aggregated_candles_to_store: List[schemas.CandleBase] = []
                for i in range(num_agg_bars):
                    # Convert Unix timestamp back to datetime
                    # Ensure it's UTC if your DB stores UTC
                    agg_dt = datetime.fromtimestamp(ts_agg[i], tz=timezone.utc) 
                    aggregated_candles_to_store.append(schemas.CandleBase(
                        timestamp=agg_dt,
                        open=o_agg[i],
                        high=h_agg[i],
                        low=l_agg[i],
                        close=c_agg[i],
                        volume=v_agg[i]
                    ))
                
                # 5. Store aggregated data
                if aggregated_candles_to_store:
                    crud_historical_data.create_ohlc_records(
                        db, aggregated_candles_to_store, exchange, token, target_interval
                    )
                    logging.info(f"Successfully Numba/CUDA resampled and stored {num_agg_bars} {target_interval} bars for {exchange}:{token}.")
                else:
                    logging.info(f"Numba/CUDA resampling to {target_interval} produced no bars for {exchange}:{token}.")
            else:
                 logging.info(f"Numba/CUDA resampling to {target_interval} produced 0 bars for {exchange}:{token}.")

        logging.info(f"Celery task: Finished Numba/CUDA aggregation for {exchange}:{token} from {start_time_dt} to {end_time_dt}")
    except Exception as e:
        logging.error(f"Error in Numba/CUDA generate_aggregates_for_range_task for {exchange}:{token}: {e}", exc_info=True)
    finally:
        db.close()