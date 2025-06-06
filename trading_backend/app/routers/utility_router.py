# app/routers/utility_router.py
from fastapi import APIRouter, HTTPException
from typing import Dict, List
import os
import uuid
import time
from ..core.cache import redis_client
from .. import schemas

router = APIRouter(
    prefix="/utils",
    tags=["Utilities"]
)

# # In a real app, load this at startup and cache it.
# # For simplicity, loading on demand here.
# SCRIPMASTER_DIR = "Scripmaster" # Assuming it's in the project root or accessible path

# def load_symbols_from_file(exchange_code: str) -> List[str]:
#     # Ensure exchange_code is safe to use in a file path
#     if not exchange_code.isalnum() or '..' in exchange_code:
#         return []
        
#     file_path = os.path.join(SCRIPMASTER_DIR, f"{exchange_code.upper()}_symbols.txt")
#     symbols = []
#     try:
#         if os.path.exists(file_path): # Check if file exists
#             with open(file_path, 'r') as f:
#                 symbols = [line.strip() for line in f if line.strip()]
#     except Exception as e:
#         print(f"Error loading symbols for {exchange_code}: {e}")
#         # Potentially log the error, but return empty list or raise specific HTTP error
#     return symbols

# SUPPORTED_EXCHANGES = ["NSE", "BSE", "NFO", "MCX"] # Example list

# @router.get("/exchanges", response_model=List[str])
# async def get_supported_exchanges():
#     """Returns a list of supported exchanges."""
#     return SUPPORTED_EXCHANGES

# @router.get("/symbols/{exchange_code}", response_model=List[str])
# async def get_symbols_for_exchange(exchange_code: str):
#     """
#     Returns a list of trading symbols/tokens for the given exchange.
#     Reads from local Scripmaster files (e.g., NSE_symbols.txt).
#     """
#     if exchange_code.upper() not in SUPPORTED_EXCHANGES:
#         raise HTTPException(status_code=404, detail=f"Exchange code '{exchange_code}' not supported or invalid.")
        
#     symbols = load_symbols_from_file(exchange_code)
#     if not symbols:
#         # Depending on requirements, either return empty list or 404
#         # raise HTTPException(status_code=404, detail=f"No symbols found or error loading for exchange {exchange_code}")
#         pass
#     return symbols

# Session Management Endpoints
@router.get("/session/initiate", response_model=schemas.SessionInfo)
def initiate_session():
    """Generates a new unique session token for the client."""
    session_token = str(uuid.uuid4())
    # Store the creation time/last heartbeat time in Redis with an expiration
    # The expiration here is a safety net. The cleanup task is the primary mechanism.
    redis_client.set(f"session:{session_token}", int(time.time()), ex=60*45) # 45 min expiry
    return schemas.SessionInfo(session_token=session_token)

@router.post("/session/heartbeat")
def session_heartbeat(session: schemas.SessionInfo):
    """Client posts to this endpoint to keep the session alive."""
    token_key = f"session:{session.session_token}"
    if redis_client.exists(token_key):
        # Update the timestamp and reset the expiration
        redis_client.set(token_key, int(time.time()), ex=60*45) # Reset TTL to 45 mins
        return {"status": "ok"}
    else:
        # If the key doesn't exist (e.g., expired or invalid token),
        # the client should probably re-initiate a session.
        return {"status": "error", "message": "Session not found or expired."}