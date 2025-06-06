from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os
from typing import Optional

load_dotenv() # take environment variables from .env.

class Settings(BaseSettings):
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    # Add other settings as needed

    # DTN IQFeed Credentials
    DTN_PRODUCT_ID: Optional[str] = os.getenv("DTN_PRODUCT_ID")
    DTN_LOGIN: Optional[str] = os.getenv("DTN_LOGIN")
    DTN_PASSWORD: Optional[str] = os.getenv("DTN_PASSWORD")

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()