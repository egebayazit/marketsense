from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

# Health
def test_health_ok():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

# Stocks
def test_stocks_calendar_ok_or_404():
    r = client.get("/stocks/AAPL", params={"days": 14})
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        data = r.json()
        assert isinstance(data, list)
        if data:
            row = data[0]
            assert "date" in row and "close" in row

def test_stocks_last_n_ok_or_404():
    r = client.get("/stocks/AAPL/last-n", params={"n": 7})
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        data = r.json()
        assert isinstance(data, list)
        if data:
            row = data[0]
            assert "date" in row and "close" in row

def test_stocks_unknown_ticker_400():
    r = client.get("/stocks/ZZZZZZ", params={"days": 7})
    assert r.status_code == 400

# News
def test_news_calendar_ok_or_404():
    r = client.get("/news", params={"days": 7})
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        data = r.json()
        assert isinstance(data, list)
        if data:
            first = data[0]
            assert {"published_at", "source", "headline", "url"} <= set(first.keys())

def test_news_last_n_ok_or_404():
    r = client.get("/news/last-n", params={"limit": 3})
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        assert len(r.json()) <= 3

def test_news_query_ok_or_404():
    r = client.get("/news", params={"days": 7, "q": "earnings", "limit": 5})
    assert r.status_code in (200, 404)
