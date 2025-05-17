FROM apache/airflow:2.7.2

USER airflow

RUN pip install --no-cache-dir \
    snowflake-connector-python \
    pandas \
    psycopg2-binary
