import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator
import sys
sys.path.insert(0, '/usr/local/airflow/') # Import the modules from the path
from include.profiles import venv_execution_config, profile_config_nyctaxi, project_config_nyctaxi, project_path_nyctaxi, dir_profiles_path
from cosmos import DbtTaskGroup

activate_venv = "source /usr/local/airflow/dbt_venv/bin/activate &&"

DB_AND_SCHEMA_CREATION_SQL = """
DROP DATABASE IF EXISTS BRONZE;
DROP DATABASE IF EXISTS SILVER;
DROP DATABASE IF EXISTS GOLD;
CREATE DATABASE IF NOT EXISTS BRONZE;
CREATE DATABASE IF NOT EXISTS SILVER;
CREATE DATABASE IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS BRONZE.NYCTAXI;
CREATE SCHEMA IF NOT EXISTS SILVER.NYCTAXI;
CREATE SCHEMA IF NOT EXISTS GOLD.NYCTAXI;
"""

with DAG(
    dag_id="elt_yellowtaxi_pipeline_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["yellowtaxi", "parquet", "google-drive", "postgres", "airbyte", "dbt", "snowflake"]

) as dag:

    start = EmptyOperator(
        task_id="start_task")

    create_db_and_schema = SQLExecuteQueryOperator(
            task_id="create_db_and_schema",
            conn_id="snowflake_default",
            sql=DB_AND_SCHEMA_CREATION_SQL
        )
    
    parquet1_to_snowflake = AirbyteTriggerSyncOperator(
        task_id="parquet1_to_snowflake",
        airbyte_conn_id="airbyte_conn",
        connection_id="03f620ad-9fb7-46b8-8e2e-7e24facaa317",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    parquet2_to_snowflake = AirbyteTriggerSyncOperator(
        task_id="parquet2_to_snowflake",
        airbyte_conn_id="airbyte_conn",
        connection_id="2a03d8fb-1a06-4180-943a-b066b4248c03",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    postgres_to_snowflake = AirbyteTriggerSyncOperator(
        task_id="postgres_to_snowflake",
        airbyte_conn_id="airbyte_conn",
        connection_id="dc45fd48-8b5a-4e20-86c3-ed7d41a4f7c6",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    check_yellowtaxi_tables_exist = SnowflakeCheckOperator(
        task_id="check_yellowtaxi_tables_exist",
        sql="""
        SELECT COUNT(*) = 4
        FROM "BRONZE".INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'NYCTAXI'
          AND TABLE_NAME IN ('YELLOWTAXI_2021', 'YELLOWTAXI_2022', 'YELLOWTAXI_2023', 'YELLOWTAXI_2024');
        """,
        snowflake_conn_id="snowflake_bronze_taxi" # Make sure this connection is configured in Airflow
    )

    dbt_task_group = DbtTaskGroup(
        group_id='dbt_nyctaxi_dag',
        project_config=project_config_nyctaxi, 
        profile_config=profile_config_nyctaxi,           # #OR # profile_mapping=profile_mapping_nyctaxi,
        execution_config=venv_execution_config
        # render_config=render_config
    )

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{activate_venv} dbt deps --project-dir {project_path_nyctaxi}"
    )

    generate_dbt_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command=(
            f"source /usr/local/airflow/dbt_venv/bin/activate && "
            f"dbt docs generate --project-dir {project_path_nyctaxi} --profiles-dir {dir_profiles_path} --static"
        )
    )

    upload_dbt_docs_to_minio = BashOperator(
    task_id='upload_dbt_docs_to_minio',
    bash_command=f"""
        set -e
        mc alias set minio $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY
        mc mb minio/dbt-docs --ignore-existing
        mc anonymous set download minio/dbt-docs
        mc cp --recursive {project_path_nyctaxi}/target/ minio/dbt-docs/
        echo "Successfully uploaded and set public policy for dbt docs you can access here: http://localhost:9000/dbt-docs/index.html"
    """
    )

    end = EmptyOperator(
        task_id = "end_task")

    
    start >> create_db_and_schema >> [parquet1_to_snowflake, parquet2_to_snowflake, postgres_to_snowflake]
    
    [parquet1_to_snowflake, parquet2_to_snowflake, postgres_to_snowflake] >> check_yellowtaxi_tables_exist
    
    check_yellowtaxi_tables_exist >> dbt_task_group >> dbt_deps_task >> generate_dbt_docs >> upload_dbt_docs_to_minio >> end