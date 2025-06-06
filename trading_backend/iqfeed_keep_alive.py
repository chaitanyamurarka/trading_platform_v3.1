#! /usr/bin/env python3
# coding=utf-8

"""
This is an example that launches IQConnect.exe.
... (rest of your docstring) ...
"""

import os
import time
import argparse
import app.pyiqfeed as iq
import socket # Added for specific socket error handling
import logging # Added for better logging

from typing import Optional

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dtn_product_id: Optional[str] = os.getenv("DTN_PRODUCT_ID")
dtn_login: Optional[str] = os.getenv("DTN_LOGIN")
dtn_password: Optional[str] = os.getenv("DTN_PASSWORD")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch IQFeed.")
    parser.add_argument('--nohup', action='store_true',
                        dest='nohup', default=False,
                        help="Don't kill IQFeed.exe when this script exists.")
    parser.add_argument('--headless', action="store_true",
                        dest='headless', default=False,
                        help="Launch IQFeed in a headless XServer.")
    parser.add_argument('--control_file', action='store',
                        dest='ctrl_file', default="/tmp/stop_iqfeed.ctrl",
                        help='Stop running if this file exists.')
    arguments = parser.parse_args()

    nohup = arguments.nohup
    headless = arguments.headless
    ctrl_file = arguments.ctrl_file

    # Initialize IQ_FEED object once
    # This ensures IQ_FEED is defined for use inside the loop
    IQ_FEED: Optional[iq.FeedService] = None
    try:
        logging.info("Initializing IQFeed Service object...")
        IQ_FEED = iq.FeedService(product=dtn_product_id,
                                 version="IQFEED_LAUNCHER_V2",
                                 login=dtn_login,
                                 password=dtn_password)
    except Exception as e_init:
        logging.error(f"Failed to initialize IQFeed Service object: {e_init}", exc_info=True)
        logging.error("Script will exit as it cannot interface with IQFeed.")
        exit(1)

    # Initial launch attempt
    try:
        logging.info(f"Initial launch attempt for IQFeed (headless={headless}, nohup={nohup}). This may take a moment...")
        IQ_FEED.launch(timeout=60,
                       check_conn=True,
                       headless=headless,
                       nohup=nohup)
        logging.info("Initial IQFeed.launch command issued. IQConnect should be running.")
    except RuntimeError as e:
        logging.error(f"Failed to launch or connect to IQFeed during initial setup: {e}")
        logging.error("Please ensure IQConnect.exe can be started and login credentials are correct.")
        logging.error("The script will exit as it cannot ensure IQFeed is running for the first time.")
        exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during initial IQFeed launch: {e}", exc_info=True)
        exit(1)


    # Main loop for maintaining connection and checking control file
    while not os.path.isfile(ctrl_file):
        admin_conn = None # Ensure admin_conn is defined for potential cleanup outside try
        try:
            # **** ADDED: Ensure IQConnect.exe is running before each connection attempt ****
            try:
                if IQ_FEED: # Check if IQ_FEED was successfully initialized
                    logging.info("Ensuring IQFeed is running before attempting Admin connection...")
                    IQ_FEED.launch(timeout=30, # Shorter timeout for re-checks
                                   check_conn=True, # Still check connection after launch
                                   headless=headless,
                                   nohup=nohup)
                    logging.info("IQFeed check/re-launch command issued.")
                else:
                    logging.error("IQ_FEED service object not initialized. Cannot ensure IQFeed is running.")
                    # This state should ideally not be reached if initial IQ_FEED init fails and exits.
                    raise RuntimeError("IQFeed Service object not available for launch check.")

            except RuntimeError as e_launch_retry:
                logging.error(f"Failed to ensure IQFeed is running during retry: {e_launch_retry}. Will proceed to connection attempt anyway but might fail.")
                # Don't exit here, let the connection attempt below fail and trigger the normal retry cycle
            except Exception as e_launch_unexpected:
                logging.error(f"Unexpected error during IQFeed pre-connection launch/check: {e_launch_unexpected}", exc_info=True)
                # As above, let the normal retry cycle handle it.

            logging.info("Attempting to establish Admin connection to IQFeed...")
            admin_conn = iq.AdminConn(name="LauncherAdmin")
            admin_listener = iq.VerboseAdminListener("LauncherAdmin-listen") # Or SilentAdminListener
            admin_conn.add_listener(admin_listener)

            # The ConnConnector handles connect() and disconnect()
            with iq.ConnConnector([admin_conn]):
                logging.info("Admin connection established. Requesting client stats.")
                admin_conn.client_stats_on() # Request client stats (optional)

                logging.info(f"Monitoring for control file: {ctrl_file}. Checking every 10 seconds.")
                # Inner loop: keep checking for the control file while connected
                while not os.path.isfile(ctrl_file):
                    # The pyiqfeed library's reader thread handles incoming messages.
                    # If the connection drops, an error should be raised by the reader thread
                    # and caught by the outer except blocks.
                    # Check if admin_conn's reader thread is alive as an additional health check
                    if not admin_conn.reader_running():
                        logging.warning("AdminConn reader thread is not running. Connection might be lost.")
                        raise ConnectionResetError("AdminConn reader thread terminated unexpectedly.")
                    time.sleep(10) # Check for ctrl_file periodically
                
                # If ctrl_file is found, break the outer loop to exit
                if os.path.isfile(ctrl_file):
                    logging.info(f"Control file '{ctrl_file}' found. Initiating shutdown.")
                    break 

        except ConnectionRefusedError as e:
            logging.warning(f"Connection refused: {e}. IQFeed may not be running or ready. Retrying in 15s.")
        except ConnectionResetError as e:
            logging.warning(f"Connection reset: {e}. Connection to IQFeed lost. Retrying in 15s.")
        except socket.timeout as e:
            logging.warning(f"Socket timeout: {e}. Possible network issue or IQFeed unresponsive. Retrying in 15s.")
        except socket.error as e: # Catch other general socket errors
            logging.warning(f"A socket error occurred: {e}. Retrying in 15s.")
        except RuntimeError as e: # Catch other runtime errors that pyiqfeed might raise
            logging.error(f"A runtime error occurred: {e}. Retrying in 15s.")
        except Exception as e: # Catch any other unexpected errors
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)
            logging.info("Attempting to recover. Retrying in 15s.")
        
        finally:
            if admin_conn and admin_conn.reader_running():
                try:
                    logging.debug("Ensuring admin_conn is disconnected in finally block.")
                    admin_conn.disconnect()
                except Exception as e_disc:
                    logging.error(f"Error during explicit disconnect in finally: {e_disc}", exc_info=True)
            elif admin_conn: # If admin_conn exists but reader isn't running (e.g. connect failed before thread start)
                try:
                    logging.debug("Attempting disconnect on admin_conn even if reader wasn't running.")
                    admin_conn.disconnect() # pyiqfeed's disconnect should be safe to call
                except Exception as e_disc_alt:
                    logging.error(f"Error during alternative disconnect in finally: {e_disc_alt}", exc_info=True)

        if os.path.isfile(ctrl_file):
            break # Exit the main while loop

        logging.info("Waiting 15 seconds before next connection attempt...")
        for _ in range(15):
            if os.path.isfile(ctrl_file):
                logging.info(f"Control file '{ctrl_file}' detected during wait. Exiting retry loop.")
                break
            time.sleep(1)
        if os.path.isfile(ctrl_file): # Ensure we break outer loop if detected
            break

    # Cleanup: Remove control file if it exists
    if os.path.exists(ctrl_file):
        try:
            logging.info(f"Removing control file: {ctrl_file}")
            os.remove(ctrl_file)
        except OSError as e:
            logging.error(f"Error removing control file '{ctrl_file}': {e}")
    
    logging.info("IQFeed Keep Alive script finished.")