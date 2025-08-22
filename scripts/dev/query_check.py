import sys, pathlib

# Ensure repo root is on sys.path so "from src..." works
ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.queries import get_recent_closes, get_latest_news

if __name__ == "__main__":
    print("\n=== Helper: recent closes AAPL (7) ===")
    print(get_recent_closes("AAPL", 7))

    print("\n=== Helper: latest news (5) ===")
    print(get_latest_news(5))
