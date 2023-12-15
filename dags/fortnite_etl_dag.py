import json
from pendulum import datetime

from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.decorators import (
    dag,
    task,
) 
@dag(

    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
    },
    tags=["etl", "fortnite"],
)  
def fortnite_etl():
    """
    ### Fortnite ETL DAG
    This is DAG is used for demo purposes. It is intended to demonstrate the use case of extracting various
    Fortnite related data sets.
    """

    # Bronze jobs
    twitch_extract_to_bronze = EmptyOperator(task_id="twitch_extract_to_bronze")
    discord_extract_to_bronze = EmptyOperator(task_id="discord_extract_to_bronze")
    fortnite_api_extract_to_bronze = EmptyOperator(task_id="fortnite_api_extract_to_bronze")
    steam_extract_to_bronze = EmptyOperator(task_id="steam_extract_to_bronze")

    # Silver jobs
    twitch_to_silver = EmptyOperator(task_id="twitch_to_silver")
    discord_to_silver = EmptyOperator(task_id="discord_to_silver")
    fortnite_api_to_silver = EmptyOperator(task_id="fortnite_api_to_silver")
    steam_to_silver = EmptyOperator(task_id="steam_to_silver")

    # Gold jobs
    aggregate_gold_models = EmptyOperator(task_id="aggregate_gold_models")

    # ML predictions
    ml_predictions = EmptyOperator(task_id="ml_predictions")

    # Refresh Dashboards
    refresh_dashboards = EmptyOperator(task_id="refresh_dashboards")


    # Dependencies
    chain(
        [twitch_extract_to_bronze, discord_extract_to_bronze, fortnite_api_extract_to_bronze, steam_extract_to_bronze],
        [twitch_to_silver, discord_to_silver, fortnite_api_to_silver, steam_to_silver],
        aggregate_gold_models,
        [refresh_dashboards, ml_predictions],
    )

fortnite_etl()
