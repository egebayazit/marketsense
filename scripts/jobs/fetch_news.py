# scripts/jobs/fetch_news.py
"""
Fetch recent news via NewsAPI and idempotently upsert into SQLite `news` table.

Schema created if missing:
  news(
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    headline     TEXT NOT NULL,
    published_at TEXT NOT NULL,  -- ISO 8601 without timezone, e.g. 2025-08-21T14:05:00
    source       TEXT,
    url          TEXT UNIQUE
  )
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from newsapi import NewsApiClient

# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# -------------------------
# Defaults / Config
# -------------------------
DEFAULT_DB_REL = Path("data/marketsense.db")

LANG = "en"
TOP_HEADLINES_COUNTRIES_DAILY = ["us"]
TOP_HEADLINES_CATEGORIES = ["business", "technology", "general"]

EVERYTHING_QUERIES = [
    "economy OR inflation",
    "markets OR stocks OR equities",
    "interest rates OR central bank OR fed OR ECB",
    "earnings OR quarterly results",
    "technology OR AI OR artificial intelligence",
    "energy OR oil OR gas OR renewables",
]

PAGE_SIZE = 100

# No-op-safe UPSERT
UPSERT_SQL = """
INSERT INTO news (headline, published_at, source, url)
VALUES (?, ?, ?, ?)
ON CONFLICT(url) DO UPDATE SET
  headline     = excluded.headline,
  published_at = excluded.published_at,
  source       = excluded.source
WHERE
  COALESCE(news.headline,     '') != COALESCE(excluded.headline,     '')
  OR COALESCE(news.published_at, '') != COALESCE(excluded.published_at, '')
  OR COALESCE(news.source,     '') != COALESCE(excluded.source,     '');
"""


# -------------------------
# Path / DB helpers
# -------------------------
def _is_airflow_container() -> bool:
    # when running via our docker-compose the repo is mounted at /opt/marketsense
    try:
        return Path("/opt/marketsense").exists()
    except Exception:
        return False


def resolve_db_path(cli_db: Optional[str]) -> Path:
    """
    Resolve DB path with precedence:
      1) --db CLI
      2) MARKETSENSE_DB env (absolute or relative to repo root)
      3) default 'data/marketsense.db' under repo root

    In Airflow, repo root is '/opt/marketsense'.
    Locally, repo root is cwd (where you run the script), i.e. project root.
    """
    root = Path("/opt/marketsense") if _is_airflow_container() else Path.cwd()
    env_db = os.getenv("MARKETSENSE_DB")

    if cli_db:
        p = Path(cli_db)
        return p if p.is_absolute() else (root / p)

    if env_db:
        p = Path(env_db)
        return p if p.is_absolute() else (root / p)

    return root / DEFAULT_DB_REL


def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS news (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,
          headline     TEXT NOT NULL,
          published_at TEXT NOT NULL,
          source       TEXT,
          url          TEXT UNIQUE
        );
        CREATE INDEX IF NOT EXISTS idx_news_published_at
          ON news(published_at DESC);
        """
    )


# -------------------------
# Time helpers
# -------------------------
def iso_no_tz(dt: datetime) -> str:
    """
    Format datetime as YYYY-MM-DDTHH:MM:SS with no timezone suffix.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S")


def normalize_published_at(value: Optional[str]) -> Optional[str]:
    """
    Normalize publishedAt strings from the API into YYYY-MM-DDTHH:MM:SS (no tz).
    Accepts '2025-08-21T12:34:56Z' or '2025-08-21T12:34:56+00:00' variants.
    """
    if not value:
        return None
    try:
        if value.endswith("Z"):
            dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return iso_no_tz(dt)
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return iso_no_tz(dt)
        except ValueError:
            pass
        # Fallback: just date
        dt = datetime.strptime(value[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return iso_no_tz(dt)
    except Exception:
        return None


# -------------------------
# API call helpers
# -------------------------
def _normalize_article(a: Dict) -> Optional[Tuple[str, str, str, str]]:
    """
    Returns tuple (headline, published_at, source, url) or None if incomplete.
    """
    title = (a.get("title") or "").strip()
    published_at = normalize_published_at(a.get("publishedAt"))
    url = (a.get("url") or "").strip()
    if not title or not published_at or not url:
        return None
    source_name = (a.get("source") or {}).get("name") or ""
    return (title, published_at, source_name, url)


def fetch_top_headlines(
    client: NewsApiClient,
    country: str,
    category: str,
    page_cap: int,
    verbose: bool,
) -> Optional[List[Dict]]:
    """
    Pull Top Headlines for a given (country, category).
    Returns:
      - list[articles] on success
      - [] if empty page(s) / soft errors
      - None if rateLimited (signals caller to stop whole run)
    """
    all_articles: List[Dict] = []
    page = 1
    while True:
        try:
            resp = client.get_top_headlines(
                country=country, category=category, page=page, page_size=PAGE_SIZE
            )
        except Exception as e:
            if verbose:
                log.warning("top_headlines request error %s/%s page %s: %s", country, category, page, e)
            time.sleep(1.0)
            try:
                resp = client.get_top_headlines(
                    country=country, category=category, page=page, page_size=PAGE_SIZE
                )
            except Exception as e2:
                if verbose:
                    log.warning("top_headlines retry failed %s/%s page %s: %s", country, category, page, e2)
                return []

        if resp.get("status") != "ok":
            if verbose:
                log.warning("top_headlines non-ok %s/%s page %s: %s", country, category, page, resp)
            if isinstance(resp, dict) and resp.get("code") == "rateLimited":
                return None
            return []

        articles = resp.get("articles") or []
        if not articles:
            break
        all_articles.extend(articles)

        if len(articles) < PAGE_SIZE or page >= page_cap:
            break
        page += 1
        time.sleep(0.1)
    return all_articles


def fetch_everything(
    client: NewsApiClient,
    query: str,
    hours: int,
    page_cap: int,
    verbose: bool,
) -> Optional[List[Dict]]:
    """
    Pull Everything for a broad query within a recent lookback window.
    Returns list, [] if empty, None if rateLimited (caller should stop whole run).
    """
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(hours=hours)
    all_articles: List[Dict] = []
    page = 1
    while True:
        params = dict(
            q=query,
            from_param=iso_no_tz(from_dt),
            to=iso_no_tz(to_dt),
            language=LANG,
            sort_by="publishedAt",
            page=page,
            page_size=PAGE_SIZE,
        )
        try:
            resp = client.get_everything(**params)
        except Exception as e:
            if verbose:
                log.warning("everything request error [%s] page %s: %s", query, page, e)
            time.sleep(1.0)
            try:
                resp = client.get_everything(**params)
            except Exception as e2:
                if verbose:
                    log.warning("everything retry failed [%s] page %s: %s", query, page, e2)
                return []

        if resp.get("status") != "ok":
            if verbose:
                log.warning("everything non-ok [%s] page %s: %s", query, page, resp)
            if isinstance(resp, dict) and resp.get("code") == "rateLimited":
                return None
            return []

        articles = resp.get("articles") or []
        if not articles:
            break
        all_articles.extend(articles)

        if len(articles) < PAGE_SIZE or page >= page_cap:
            break
        page += 1
        time.sleep(0.1)
    return all_articles


# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    parser.add_argument("--db", help="Override DB path (absolute or relative to repo root)")
    args = parser.parse_args()

    # Mode knobs (keep daily gentle for free tier; widen on backfill)
    if args.mode == "backfill":
        countries = ["us", "gb", "tr"]
        page_cap = 3
        window_hours = 48
        verbose = True
    else:
        countries = TOP_HEADLINES_COUNTRIES_DAILY
        page_cap = 1
        window_hours = 24
        verbose = False

    # Env & API
    load_dotenv()
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        raise RuntimeError("Missing NEWS_API_KEY in environment (set it in your .env).")
    client = NewsApiClient(api_key=api_key)

    # DB setup
    db_path = resolve_db_path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Connecting to database at %s", db_path)

    # Collect
    rate_limited = False
    dedup_urls: set[str] = set()
    rows: List[Tuple[str, str, str, str]] = []

    # 1) Top Headlines breadth sweep
    for country in countries:
        if rate_limited:
            break
        for category in TOP_HEADLINES_CATEGORIES:
            arts = fetch_top_headlines(client, country, category, page_cap=page_cap, verbose=verbose)
            if arts is None:
                log.warning("Rate-limited during top_headlines; stopping.")
                rate_limited = True
                break
            if verbose:
                log.info("top_headlines[%s/%s]: %d", country, category, len(arts))
            for a in arts:
                tup = _normalize_article(a)
                if not tup:
                    continue
                h, pub, src, url = tup
                if url in dedup_urls:
                    continue
                dedup_urls.add(url)
                rows.append((h, pub, src, url))

    # 2) Everything thematic sweep
    if not rate_limited:
        for q in EVERYTHING_QUERIES:
            arts = fetch_everything(client, q, hours=window_hours, page_cap=page_cap, verbose=verbose)
            if arts is None:
                log.warning("Rate-limited during everything; stopping.")
                rate_limited = True
                break
            if verbose:
                log.info("everything[%s]: %d", q, len(arts))
            for a in arts:
                tup = _normalize_article(a)
                if not tup:
                    continue
                h, pub, src, url = tup
                if url in dedup_urls:
                    continue
                dedup_urls.add(url)
                rows.append((h, pub, src, url))

    # Upsert
    changed = 0
    with sqlite3.connect(db_path) as con:
        ensure_schema(con)
        if rows:
            cur = con.cursor()
            cur.executemany(UPSERT_SQL, rows)
            changed = cur.rowcount or 0
        con.commit()

    if rate_limited:
        log.warning("⚠️ Stopped early due to NewsAPI rate limit.")
    log.info("✅ Total news upserted/updated: %d (processed=%d, unique urls=%d)", changed, len(rows), len(dedup_urls))


if __name__ == "__main__":
    main()
