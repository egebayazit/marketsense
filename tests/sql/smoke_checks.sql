-- 1) Duplicate daily bars (should be 0 rows)
SELECT ticker, date, COUNT(*) AS cnt
FROM stocks
GROUP BY 1,2
HAVING COUNT(*) > 1;

-- 2) Sanity: negative close or volume < 0 (should be 0 rows)
SELECT *
FROM stocks
WHERE close < 0 OR volume < 0;

-- 3) Recent closes sample (OK to return 0 rows if no data yet)
SELECT date, close FROM stocks
WHERE ticker='AAPL'
ORDER BY date DESC
LIMIT 7;

-- 4) Latest news sample (OK to return 0 rows if no data yet)
SELECT headline, published_at, COALESCE(source,'unknown') AS source
FROM news
ORDER BY id DESC
LIMIT 5;
