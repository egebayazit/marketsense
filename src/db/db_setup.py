from pathlib import Path
import sqlite3

# Ensure /data exists
Path("data").mkdir(exist_ok=True)

DB_PATH = Path("data/marketsense.db")
MIGRATION = Path("db/sqlite/001_init.sql")

def main():
    if not MIGRATION.exists():
        raise FileNotFoundError(f"Migration not found: {MIGRATION}")
    sql = MIGRATION.read_text(encoding="utf-8")
    con = sqlite3.connect(DB_PATH)
    try:
        con.executescript(sql)
        con.commit()
        print(f"✅ DB initialized from migration -> {DB_PATH}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
