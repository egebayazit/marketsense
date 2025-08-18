import yfinance as yf

def fetch_stock_data(ticker="AAPL", period="5d", interval="1d"):
    stock = yf.Ticker(ticker)
    data = stock.history(period=period, interval=interval)

    print(f"\n✅ Fetched data for {ticker}")
    print(data[["Open", "Close", "Volume"]])

    return data

if __name__ == "__main__":
    fetch_stock_data("AAPL")
