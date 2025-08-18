import os
from dotenv import load_dotenv
from newsapi import NewsApiClient
import json
from datetime import datetime

# 🔹 import DB helper
from src.db.ops import insert_news_rows

# Load .env file
load_dotenv()

# Get NewsAPI key
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

# Initialize News API client
newsapi = NewsApiClient(api_key=NEWS_API_KEY)

# Safety check
if not NEWS_API_KEY:
    raise ValueError("NEWS_API_KEY is missing. Please set it in your .env file.")


def fetch_financial_headlines(query="finance", page_size=10):
    try:
        response = newsapi.get_everything(
            q=query,
            language="en",
            sort_by="publishedAt",
            page_size=page_size,
        )
        articles = response.get("articles", [])

        cleaned_articles = []
        for article in articles:
            cleaned_articles.append({
                "title": article["title"],
                "description": article["description"],
                "publishedAt": article["publishedAt"],
                "source": article["source"]["name"],
                "url": article["url"]
            })

        return cleaned_articles

    except Exception as e:
        print(f"Error fetching news: {e}")
        return []


def save_articles_to_json(articles, filename="data/news_raw.json"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)

    print(f"\n✅ Saved {len(articles)} articles to {filename}")


# 🔹 NEW: save to DB
def save_articles_to_db(articles):
    rows = []
    for article in articles:
        published_at = article["publishedAt"]

        # Ensure consistent timestamp format
        try:
            dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            published_at = dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            published_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows.append({
            "headline": article["title"],
            "published_at": published_at,
            "source": article.get("source"),
            "url": article.get("url"),
            "ticker": None,  # future step: NLP/keyword mapping
        })

    insert_news_rows(rows)
    print(f"✅ Inserted {len(rows)} news rows into DB (duplicates skipped)")


if __name__ == "__main__":
    headlines = fetch_financial_headlines()
    save_articles_to_json(headlines)

    # 🔹 save into DB as well
    save_articles_to_db(headlines)

    for i, article in enumerate(headlines, start=1):
        print(f"\n{i}. {article['title']}\n{article['description']}")
