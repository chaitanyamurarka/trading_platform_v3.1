# app/dtn_iq_client.py
import logging
from . import pyiqfeed as iq
from .config import settings
import time
import os

is_iqfeed_service_launched = False
iqfeed_launch_error = None

def _check_admin_port_connectivity():
    """
    Tries to connect to the IQFeed admin port.
    Returns True if successful, False otherwise, along with an error message if any.
    """
    try:
        conn_check = iq.AdminConn(name="AdminPortCheckInternal")
        conn_check.connect() # Defaults to localhost:9300
        # A very short sleep to see if connection holds, then check attribute
        time.sleep(0.5) # Reduced from 1-2 seconds for a quicker check
        if conn_check.connected:
            logging.debug("Admin port check: Successfully connected.")
            conn_check.disconnect()
            return True, None
        else:
            # conn_check.disconnect() # Clean up
            logging.warning("Admin port check: connect() called but 'connected' attribute is False.")
            return False, "AdminConn reported not connected after connect() call."
    except ConnectionRefusedError as e_refused:
        logging.warning(f"Admin port check: ConnectionRefusedError: {e_refused}")
        return False, f"ConnectionRefusedError: {e_refused}"
    except Exception as e_conn:
        logging.error(f"Admin port check: Exception during connection attempt: {e_conn}", exc_info=True)
        return False, f"Exception during admin port connection attempt: {e_conn}"

def launch_iqfeed_service_if_needed(force_launch_attempt=False):
    global is_iqfeed_service_launched, iqfeed_launch_error

    if is_iqfeed_service_launched and not force_launch_attempt:
        # If already marked as launched and we are not forcing a re-launch,
        # still quickly verify connectivity as it might have idled out.
        # This is more for the get_iqfeed_history_conn scenario.
        # For initial startup, this block might be skipped if is_iqfeed_service_launched is False.
        logging.debug("launch_iqfeed_service_if_needed: Already marked as launched, verifying connectivity.")
        is_connected_now, _ = _check_admin_port_connectivity()
        if is_connected_now:
            logging.debug("launch_iqfeed_service_if_needed: Connectivity verified.")
            return # Still good
        else:
            logging.warning("launch_iqfeed_service_if_needed: Was marked launched, but connectivity check failed. Proceeding to re-launch.")
            is_iqfeed_service_launched = False # Mark as not launched to force attempt
            # Fall through to attempt launch

    # Reset previous error for a fresh attempt
    iqfeed_launch_error = None

    if not all([settings.DTN_PRODUCT_ID, settings.DTN_LOGIN, settings.DTN_PASSWORD]):
        iqfeed_launch_error = "DTN IQFeed credentials not fully configured."
        logging.error(iqfeed_launch_error)
        is_iqfeed_service_launched = False
        return

    # Try to launch IQFeed client via FeedService
    logging.info("Attempting to launch/ensure IQFeed client is running via FeedService...")
    try:
        svc = iq.FeedService(
            product=settings.DTN_PRODUCT_ID,
            version="1.1_TradingApp_FastAPI",
            login=settings.DTN_LOGIN,
            password=settings.DTN_PASSWORD
        )
        logging.info("Issuing FeedService.launch(headless=False)... (Observe EC2 desktop for GUI)")
        svc.launch(headless=False) # Keep headless=False for now for visual debugging on EC2
        logging.info("IQFeed FeedService.launch() command issued. Waiting for client to initialize (1s)...")
        time.sleep(1)

        # After attempting launch, check connectivity
        is_connected_after_launch, conn_error_msg = _check_admin_port_connectivity()
        if is_connected_after_launch:
            logging.info("Successfully connected to IQFeed admin port after FeedService launch/check.")
            is_iqfeed_service_launched = True
            iqfeed_launch_error = None
        else:
            iqfeed_launch_error = (f"Failed to connect after FeedService.launch(). Error: {conn_error_msg}. "
                                   "Ensure IQLink.exe started (check EC2 desktop), logged in without issues, "
                                   "and Windows Firewall is not blocking localhost:9300.")
            logging.error(iqfeed_launch_error)
            is_iqfeed_service_launched = False

    except Exception as e_launch:
        iqfeed_launch_error = f"Exception during FeedService.launch() or connection check: {e_launch}"
        logging.error(iqfeed_launch_error, exc_info=True)
        is_iqfeed_service_launched = False
    
    if not is_iqfeed_service_launched:
        logging.warning(f"IQFeed service could not be confirmed as running. Final error: {iqfeed_launch_error}")


def get_iqfeed_history_conn() -> iq.HistoryConn | None:
    global is_iqfeed_service_launched, iqfeed_launch_error

    # Step 1: Perform a live connectivity check, as IQFeed might have idled out
    logging.debug("get_iqfeed_history_conn: Performing live connectivity check to IQFeed admin port.")
    is_connected_now, conn_err = _check_admin_port_connectivity()

    if not is_connected_now:
        logging.warning(f"get_iqfeed_history_conn: Live connectivity check failed ({conn_err}). Attempting to (re)launch IQFeed service.")
        # Mark as not launched to ensure launch_iqfeed_service_if_needed tries fully
        is_iqfeed_service_launched = False 
        launch_iqfeed_service_if_needed(force_launch_attempt=True) # Force an attempt to bring it up
        
        if not is_iqfeed_service_launched: # Check status after the launch attempt
            logging.error(f"get_iqfeed_history_conn: Failed to establish IQFeed service after re-launch attempt. Error: {iqfeed_launch_error}")
            return None
        else:
            logging.info("get_iqfeed_history_conn: IQFeed service (re)established successfully.")
    else:
        # If already connected, ensure is_iqfeed_service_launched is also true
        if not is_iqfeed_service_launched:
            logging.info("get_iqfeed_history_conn: Live connectivity okay, updating global launched status.")
            is_iqfeed_service_launched = True 
            iqfeed_launch_error = None


    # Step 2: If service is (now) marked as launched, create and return HistoryConn
    if is_iqfeed_service_launched:
        try:
            hist_conn = iq.HistoryConn(name="TradingAppHistConn")
            logging.info("get_iqfeed_history_conn: IQFeed HistoryConn instance created.")
            return hist_conn
        except Exception as e:
            logging.error(f"get_iqfeed_history_conn: Failed to create IQFeed HistoryConn instance: {e}", exc_info=True)
            return None
    else:
        # This case should ideally be caught by the check above, but as a fallback:
        logging.error("get_iqfeed_history_conn: IQFeed service is not launched, cannot create HistoryConn.")
        return None
    
# Add a getter function for clarity if preferred, or import directly
def get_iqfeed_service_status():
    return is_iqfeed_service_launched, iqfeed_launch_error