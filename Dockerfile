# Single-stage for now — multi-stage optimisation is a day-7 task, not day 1.
FROM apache/airflow:3.3.0-python3.12

COPY requirements-app.txt /requirements-app.txt
RUN pip install --no-cache-dir -r /requirements-app.txt
