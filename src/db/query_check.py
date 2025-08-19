from src.db.queries import get_recent_closes, get_latest_news

if __name__ == "__main__":
    print("\n=== Helper: recent closes AAPL (7) ===")
    print(get_recent_closes("AAPL", 7))

    print("\n=== Helper: latest news (5) ===")
    print(get_latest_news(5))
