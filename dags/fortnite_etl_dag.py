import json
from pendulum import datetime

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

    @task()
    def twitch_extract_to_bronze():
        print("Extracting Fortnite Twitch data to bronze layer.")

    @task()
    def discord_extract_to_bronze():
        print("Extracting Fortnite Discord data to bronze layer.")

    @task()
    def fortnite_api_extract_to_bronze():
        print("Extracting public Fortnite data via API to bronze layer.")

    @task()
    def steam_extract_to_bronze():
        print("Extracting Fortnite Steam data to bronze layer.")

    @task()
    def twitch_to_silver():
        print("Performing filtering, formatting, and deduplication transformations on Twitch data into silver layer.")

    @task()
    def discord_to_silver():
        print("Performing filtering, formatting, and deduplication transformations on Discord data into silver layer.")

    @task()
    def fortnite_api_to_silver():
        print("Performing filtering, formatting, and deduplication transformations on public Fortnite API data into silver layer.")

    @task()
    def steam_to_silver():
        print("Performing filtering, formatting, and deduplication transformations on Steam data into silver layer.")

    @task()
    def aggregate_gold_models():
        print("Calling DBT scripts to create dimensional models from the data in silver.")


    # Dependencies
    chain(
        [twitch_extract_to_bronze, discord_extract_to_bronze, fortnite_api_extract_to_bronze, steam_extract_to_bronze],
        [twitch_to_silver, discord_to_silver, fortnite_api_to_silver, steam_to_silver],
        aggregate_gold_models,
    )

fortnite_etl()
