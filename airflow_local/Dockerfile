FROM apache/airflow:2.9.3

# Copy your extra requirements into the image
COPY requirements_ingest.txt /requirements_ingest.txt

# Use the official hook (installs as airflow user, not root)
RUN pip install --no-cache-dir -r /requirements_ingest.txt
