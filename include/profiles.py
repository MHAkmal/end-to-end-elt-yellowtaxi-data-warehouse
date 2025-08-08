from cosmos import ProfileConfig, ExecutionConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping, SnowflakeUserPasswordProfileMapping
from pathlib import Path

dir_profiles_path = "/usr/local/airflow/.dbt"
dbt_executable_path=str(Path('/usr/local/airflow/dbt_venv/bin/dbt')) # Path dbt executable
profiles_yml_filepath=Path(f"{dir_profiles_path}/profiles.yml").as_posix() # Path dbt profile


#####################       [nyctaxi] dbt project profile and configuration       ########################

project_path_nyctaxi=Path('/usr/local/airflow/dags/dbt_project/nyctaxi').as_posix() # Path dbt project

venv_execution_config = ExecutionConfig(
        dbt_executable_path=dbt_executable_path,
        )

profile_config_nyctaxi = ProfileConfig(
    profile_name='nyctaxi', # Change the value to the dbt project name
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_bronze_taxi"),
    # profiles_yml_filepath=profiles_yml_filepath
)

project_config_nyctaxi = ProjectConfig(project_path_nyctaxi)

# Use profile_mapping for using connection stated in airflow or airflow UI

# If you want to run a specific model, skip another, or include tests, you can add RenderConfig in “include/profiles.py”:

# render_config=RenderConfig(
#           select=["path:models/X"],                   # Run only models in X/
#           exclude=["path:seeds/"],                    # Avoid models in seeds/
#           test_behavior=TestBehavior.AFTER_ALL,       # Test after running all the models
#         )