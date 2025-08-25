# airflow/dags/marketsense_daily_ingest.py
"""
MarketSense daily ingest DAG
Order: fetch_prices -> fetch_news
Runs your repo scripts inside the Airflow containers.

Notes:
- Your repo is mounted at /opt/marketsense in docker-compose.
- We keep an optional venv activation (Linux layout) if you add one later.
"""

from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# --- Paths inside the container ------------------------------------------------
# Repo is mounted to /opt/marketsense (see docker-compose).
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", "/opt/marketsense")).resolve()

# Optional: if you later create a venv in the repo (Linux layout), we’ll source it.
VENV_ACTIVATE = PROJECT_ROOT / ".venv" / "bin" / "activate"

# --- Environment passthrough (keeps keys out of logs) --------------------------
ENV = {
    "NEWS_API_KEY": os.environ.get("NEWS_API_KEY", ""),
    # Add other env you want to pass through here
}

# --- DAG defaults --------------------------------------------------------------
default_args = {
    "owner": "marketsense",
    "retries": 0,                       # set >0 after things are stable
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marketsense_daily_ingest",
    description="Run prices then news ingestion once daily",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 8, 24),
    catchup=False,                      # avoid backfilling while developing
    dagrun_timeout=timedelta(minutes=30),
    tags=["marketsense", "ingest"],
) as dag:

    # Safe bash prelude: pipefail + optional venv activation (Linux path)
    bash_prelude = f"""
set -euo pipefail
if [ -f "{VENV_ACTIVATE}" ]; then
  # Optional: use your project venv if you later add one to the image/volume
  . "{VENV_ACTIVATE}"
fi
"""

    fetch_prices = BashOperator(
        task_id="fetch_prices",
        bash_command=f"""
{bash_prelude}
python "{PROJECT_ROOT}/scripts/jobs/fetch_prices.py"
""",
        env=ENV,
    )

    fetch_news = BashOperator(
        task_id="fetch_news",
        bash_command=f"""
{bash_prelude}
python "{PROJECT_ROOT}/scripts/jobs/fetch_news.py" --mode daily
""",
        env=ENV,
    )

    fetch_prices >> fetch_news
