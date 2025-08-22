import os, re, json, pathlib

ROOT = pathlib.Path(".").resolve()
REPORT = {
    "files": [],
    "suspicions": {
        "python_schema_creation": [],
        "hardcoded_sqlite_urls": [],
        "alpha_vantage_mentions": [],
        "newsapi_mentions": [],
        "large_data_files": [],
        "dev_scripts_in_src_db": [],
    }
}

PY_SCHEMA_PATTERNS = [
    r"\bmetadata\.create_all\(",
    r"\bBase\.metadata\.create_all\(",
    r"\bTable\s*\(",
    r"CREATE TABLE",  # raw SQL in .py
]
SQLITE_URL_PAT = r"create_engine\((?:\"|')sqlite:\/\/\/[^\"']+(?:\"|')"
MENTION_PATTERNS = {
    "alpha_vantage_mentions": r"Alpha\s*Vantage",
    "newsapi_mentions": r"\bNewsAPI\b",
}

def scan():
    for path in ROOT.rglob("*"):
        if any(part in {".git", ".venv", "venv", "__pycache__"} for part in path.parts):
            continue
        rel = path.as_posix()
        if path.is_file():
            size = path.stat().st_size
            REPORT["files"].append({"path": rel, "size": size})

            # flag big data files (>5MB)
            if rel.startswith("data/") and size > 5 * 1024 * 1024:
                REPORT["suspicions"]["large_data_files"].append({"path": rel, "size": size})

            if path.suffix == ".py":
                txt = path.read_text(encoding="utf-8", errors="ignore")

                # schema creation in python
                if any(re.search(p, txt) for p in PY_SCHEMA_PATTERNS):
                    # exclude our known migration runner (db_setup allowed only if it executes SQL file)
                    if "executescript(" not in txt and "001_init.sql" not in txt:
                        REPORT["suspicions"]["python_schema_creation"].append(rel)

                # hardcoded sqlite urls
                if re.search(SQLITE_URL_PAT, txt):
                    REPORT["suspicions"]["hardcoded_sqlite_urls"].append(rel)

                # old mentions
                for key, pat in MENTION_PATTERNS.items():
                    if re.search(pat, txt, flags=re.IGNORECASE):
                        REPORT["suspicions"][key].append(rel)

            # dev scripts accidentally inside src/db
            if rel.startswith("src/db/") and any(rel.endswith(n) for n in ["add_indexes.py", "check_db.py", "db_setup_old.py"]):
                REPORT["suspicions"]["dev_scripts_in_src_db"].append(rel)

def main():
    scan()
    # Summary print
    print("\n== Repo Audit Summary ==")
    print(f"Files scanned: {len(REPORT['files'])}")

    for k, v in REPORT["suspicions"].items():
        print(f"\n-- {k} ({len(v)}) --")
        for item in v[:50]:
            print("  ", item)

    # Save full JSON for reference
    out = ROOT / "scripts" / "dev" / "repo_audit_report.json"
    out.write_text(json.dumps(REPORT, indent=2))
    print(f"\nWrote full report: {out}")

if __name__ == "__main__":
    main()
