from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()

# API Keys
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")

# Qdrant
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))

# Debug
if not NEWS_API_KEY:
    print("⚠️ NEWS_API_KEY not set in .env")
if not ALPHA_VANTAGE_KEY:
    print("⚠️ ALPHA_VANTAGE_KEY not set in .env")
