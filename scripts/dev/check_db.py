from pathlib import Path
from sqlalchemy import create_engine, inspect, text
import os

DB_STR = os.getenv("MARKETSENSE_DB_URL", "sqlite:///data/marketsense.db")


print("cwd:", Path().resolve())
print("DB URL:", DB_STR)
print("DB exists on disk:", Path("data/marketsense.db").resolve().exists())

engine = create_engine(DB_STR, future=True)
insp = inspect(engine)
tables = insp.get_table_names()
print("tables:", tables)

# optional: row counts per table
with engine.connect() as conn:
    for t in tables:
        try:
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"{t}: {cnt} rows")
        except Exception as e:
            print(f"{t}: error counting rows -> {e}")
