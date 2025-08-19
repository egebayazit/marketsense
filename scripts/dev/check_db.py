# src/db/check_db.py
from sqlalchemy import create_engine, inspect

engine = create_engine("sqlite:///data/marketsense.db", future=True)
insp = inspect(engine)
print(insp.get_table_names())
