FROM astrocrpublic.azurecr.io/runtime:3.0-6

RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install dbt-postgres dbt-snowflake dbt-bigquery dbt-redshift dbt-duckdb

COPY --chown=50000:0 ./dags/dbt_project/nyctaxi /usr/local/airflow/dags/dbt_project/nyctaxi

USER 50000
WORKDIR /usr/local/airflow/dags/dbt_project/nyctaxi
RUN /usr/local/airflow/dbt_venv/bin/dbt deps

USER root
RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc
WORKDIR /usr/local/airflow