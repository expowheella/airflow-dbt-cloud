FROM quay.io/astronomer/astro-runtime:11.4.0
# FROM quay.io/astronomer/astro-runtime:11.3.0-base


RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery==1.5.3 airflow-dbt-python && deactivate