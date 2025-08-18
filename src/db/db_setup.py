# src/db/db_setup.py
from pathlib import Path
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Date, Float, Text, DateTime
)

# Ensure /data exists (safe even if it already does)
Path("data").mkdir(exist_ok=True)

# SQLite file under /data
ENGINE_URL = "sqlite:///data/marketsense.db"
engine = create_engine(ENGINE_URL, future=True)
metadata = MetaData()

# ---------- tables ----------
stocks = Table(
    "stocks", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(16), index=True, nullable=False),
    Column("date", Date, index=True, nullable=False),
    Column("open", Float),
    Column("close", Float),
    Column("volume", Integer),
)

news = Table(
    "news", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(16), index=True, nullable=True),  # optional mapping
    Column("headline", Text, nullable=False),
    Column("source", String(64)),
    Column("published_at", DateTime, index=True),
    Column("url", Text),
)

# Create tables if they don't exist
metadata.create_all(engine)
print("✅ DB ready at data/marketsense.db with tables: stocks, news")
