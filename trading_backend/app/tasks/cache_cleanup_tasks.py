# app/tasks/cache_cleanup_tasks.py
from .celery_app import celery_application
from app.core.cache import redis_client
import time
import logging

SESSION_TIMEOUT_SECONDS = 30 * 60  # 30 minutes

@celery_application.task(name="tasks.cleanup_expired_sessions")
def cleanup_expired_sessions_task():
    """
    Scans for all session keys, checks if they have expired, and if so,
    deletes the session key and all associated user data from the cache.
    """
    logging.info("Starting expired session cleanup task...")
    try:
        session_keys = [key.decode('utf-8') for key in redis_client.scan_iter("session:*")]
        current_time = int(time.time())
        expired_sessions_count = 0
        deleted_data_keys_count = 0

        for session_key in session_keys:
            last_seen_timestamp_bytes = redis_client.get(session_key)
            if last_seen_timestamp_bytes:
                last_seen_timestamp = int(last_seen_timestamp_bytes)
                if current_time - last_seen_timestamp > SESSION_TIMEOUT_SECONDS:
                    # Session has expired
                    expired_sessions_count += 1
                    session_token = session_key.split(":")[1]
                    logging.info(f"Session {session_token[:8]}... expired. Deleting associated data.")

                    # Delete the session key itself
                    redis_client.delete(session_key)

                    # Find and delete associated user data keys
                    user_data_pattern = f"user:{session_token}:*"
                    # Use a pipeline for bulk deletion for efficiency
                    pipe = redis_client.pipeline()
                    keys_to_delete = [key for key in redis_client.scan_iter(user_data_pattern)]
                    if keys_to_delete:
                        for key in keys_to_delete:
                            pipe.delete(key)
                        pipe.execute()
                        deleted_data_keys_count += len(keys_to_delete)
                        logging.debug(f"Deleted {len(keys_to_delete)} data keys for expired session.")

        logging.info(f"Cleanup task finished. Found {len(session_keys)} total sessions. "
                     f"Cleaned up {expired_sessions_count} expired sessions and deleted {deleted_data_keys_count} associated data keys.")
        return {"status": "success", "sessions_cleaned": expired_sessions_count, "data_keys_deleted": deleted_data_keys_count}

    except Exception as e:
        logging.error(f"Error during cache cleanup task: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}