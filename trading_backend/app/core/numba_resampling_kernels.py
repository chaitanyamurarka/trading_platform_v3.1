import numpy as np
import numba

@numba.jit(nopython=True)
def resample_ohlc_cpu_jit(
    timestamps_1s, open_1s, high_1s, low_1s, close_1s, volume_1s,
    out_timestamps, out_open, out_high, out_low, out_close, out_volume,
    aggregation_seconds: int
):
    """
    Performs OHLC resampling on sorted 1-second data using Numba for CPU acceleration.
    """
    if len(timestamps_1s) == 0:
        return 0

    # Initialize variables for the first aggregated bar
    current_bar_idx = 0
    # Determine the start of the very first time bucket
    bucket_start_ts = np.floor(timestamps_1s[0] / aggregation_seconds) * aggregation_seconds
    
    # Set the timestamp and open for the first bar
    out_timestamps[current_bar_idx] = bucket_start_ts
    out_open[current_bar_idx] = open_1s[0]
    out_high[current_bar_idx] = high_1s[0]
    out_low[current_bar_idx] = low_1s[0]
    out_close[current_bar_idx] = close_1s[0]
    out_volume[current_bar_idx] = volume_1s[0]

    # Loop through the rest of the 1-second data
    for i in range(1, len(timestamps_1s)):
        ts = timestamps_1s[i]
        
        # Check if this 1s bar belongs to a new time bucket
        if ts >= bucket_start_ts + aggregation_seconds:
            # Current bar is complete, start a new one
            current_bar_idx += 1
            bucket_start_ts = np.floor(ts / aggregation_seconds) * aggregation_seconds
            
            # Initialize the new bar
            out_timestamps[current_bar_idx] = bucket_start_ts
            out_open[current_bar_idx] = open_1s[i]
            out_high[current_bar_idx] = high_1s[i]
            out_low[current_bar_idx] = low_1s[i]
            out_volume[current_bar_idx] = 0.0 # Reset volume for summing
        
        # Update high, low, close, and volume for the current aggregated bar
        if high_1s[i] > out_high[current_bar_idx]:
            out_high[current_bar_idx] = high_1s[i]
        
        if low_1s[i] < out_low[current_bar_idx]:
            out_low[current_bar_idx] = low_1s[i]
            
        out_close[current_bar_idx] = close_1s[i] # Always update with the latest close
        out_volume[current_bar_idx] += volume_1s[i]

    # Return the total number of complete aggregated bars created
    # The actual count is index + 1
    return current_bar_idx + 1


def launch_resample_ohlc(
    timestamps_1s: np.ndarray, open_1s: np.ndarray, high_1s: np.ndarray, 
    low_1s: np.ndarray, close_1s: np.ndarray, volume_1s: np.ndarray,
    aggregation_seconds: int
) -> tuple:

    num_1s_records = len(timestamps_1s)
    if num_1s_records == 0:
        return (np.array([]),) * 6 + (0,) # Return 6 empty arrays and count 0

    # Estimate max possible output bars. A generous upper bound is num_1s_records.
    max_out_bars = num_1s_records

    # Allocate output host arrays
    out_timestamps_host = np.zeros(max_out_bars, dtype=np.float64)
    out_open_host = np.zeros(max_out_bars, dtype=np.float64)
    out_high_host = np.zeros(max_out_bars, dtype=np.float64)
    out_low_host = np.zeros(max_out_bars, dtype=np.float64)
    out_close_host = np.zeros(max_out_bars, dtype=np.float64)
    out_volume_host = np.zeros(max_out_bars, dtype=np.float64)

    # Call the Numba JIT-compiled function
    actual_bars = resample_ohlc_cpu_jit(
        timestamps_1s, open_1s, high_1s, low_1s, close_1s, volume_1s,
        out_timestamps_host, out_open_host, out_high_host, out_low_host, out_close_host, out_volume_host,
        aggregation_seconds
    )
    
    # Trim the output arrays to the actual number of bars produced
    return (
        out_timestamps_host[:actual_bars], 
        out_open_host[:actual_bars], 
        out_high_host[:actual_bars], 
        out_low_host[:actual_bars], 
        out_close_host[:actual_bars], 
        out_volume_host[:actual_bars],
        actual_bars
    )