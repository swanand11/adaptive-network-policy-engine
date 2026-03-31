import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent
class Config:
    MONGO_URI = os.getenv("MONGO_URI")
    KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER")
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
    REDIS_HOST= os.getenv("REDIS_HOST")
    REDIS_PORT=os.getenv("REDIS_PORT")
