# src/data_ingestion/stock_to_db.py
from datetime import datetime, timedelta
from typing import List, Dict

import yfinance as yf

from src.db.ops import insert_stock_rows

def fetch_stock_rows(ticker: str, days: int = 10) -> List[Dict]:
    """
    Fetch last `days` daily bars for the ticker and return rows compatible with DB.
    """
    end = datetime.today().date()
    start = end - timedelta(days=days + 2)  # small buffer for weekends/holidays
    df = yf.download(ticker, start=start.isoformat(), end=end.isoformat(), interval="1d", progress=False)

    rows: List[Dict] = []
    if df is None or df.empty:
        return rows

    # Normalize column names used by yfinance
    # yfinance daily columns: ['Open','High','Low','Close','Adj Close','Volume']
    for ts, row in df.iterrows():
        # Skip NaNs (non-trading days can appear depending on calendar)
        if str(row.get("Close")) == "nan":
            continue
        rows.append({
            "ticker": ticker.upper(),
            "date": ts.date().isoformat(),  # 'YYYY-MM-DD'
            "open": float(row["Open"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]) if not str(row["Volume"]) == "nan" else 0,
        })
    return rows

def main():
    # change/extend this list as needed
    tickers = ["AAPL", "MSFT", "GOOGL"]

    all_rows: List[Dict] = []
    for t in tickers:
        rows = fetch_stock_rows(t, days=14)
        all_rows.extend(rows)

    insert_stock_rows(all_rows)
    print(f"✅ inserted {len(all_rows)} stock rows (duplicates auto-ignored)")

if __name__ == "__main__":
    main()
