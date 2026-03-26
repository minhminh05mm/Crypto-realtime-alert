import os
from dotenv import load_dotenv

load_dotenv("../.env")

TOKEN = os.getenv("TELEGRAM_TOKEN")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")