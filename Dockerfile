FROM apache/airflow:slim-2.9.0-python3.9
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-postgres kafka-python