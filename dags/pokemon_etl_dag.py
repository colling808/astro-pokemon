
from pendulum import datetime
import requests
import json
import csv
from datetime import datetime as dt

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
    tags=["etl", "advanced"],
) 
def pokemon_etl():
    """
    ### Pokemon ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using tasks for extract, transform, and load.
    """

    @task()
    def extract_transform():
        """
        #### Extract & Transform Task
        This task paginates through the PokeAPI to return a list of data from the first 151 Pokemon.
        """
        pokemon_data = []
        url = 'https://pokeapi.co/api/v2/pokemon/'

        # Paginate through each API call for the first 151 Pokemon
        for i in range(1, 152): 
            response = requests.get(url + str(i))
            if response.status_code == 200:
                
                pokemon_resp = response.json()

                # Extracting type(s) for Pokémon
                types = pokemon_resp["types"]
                type1 = types[0]["type"]["name"]
                type2 = types[1]["type"]["name"] if len(types) > 1 else ''  # If there's no second type, leave it blank'
                
                # Extracting abilit(ies) for Pokemon
                abilities = pokemon_resp["abilities"]
                ability1 = abilities[0]["ability"]["name"]
                ability2 = abilities[1]["ability"]["name"] if len(abilities) > 1 else ''  # If there's no second ability, leave it blank'

                # Extract stats for Pokemon
                stats = pokemon_resp["stats"]
                hp = stats[0]["base_stat"]
                attack = stats[1]["base_stat"]
                defense = stats[2]["base_stat"]
                special_attack = stats[3]["base_stat"]
                special_defense = stats[4]["base_stat"]
                speed = stats[5]["base_stat"]
                total_stats = hp + attack + defense + special_attack + special_defense + speed

                pokemon_info = {
                    "id": pokemon_resp["id"],
                    "name": pokemon_resp['name'],
                    "height": pokemon_resp["height"],
                    "weight": pokemon_resp["weight"],
                    "type1": type1,
                    "type2": type2,
                    "ability1": ability1,
                    "ability2": ability2,
                    "total_stats": total_stats,
                    "hp": hp,
                    "attack": attack,
                    "defense": defense,
                    "special_attack": special_attack,
                    "special_defense": special_defense,
                    "speed": speed,
                }

                pokemon_data.append(pokemon_info)

            else:
                raise ValueError(f"Failed to fetch data for Pokemon {i}. Status code: {response.status_code}")

        return pokemon_data

    @task()
    def load(pokemon_data: list, prefix="/tmp"):
        """
        #### Load Task
        Given a formatted list of list of Pokemon data, loads the data into a header-formatted csv file.
        """

        headers = ["id", "name", "height", "weight", "type1", "type2", "ability1", "ability2", "total_stats", "hp", "attack", "defense", "special_attack", "special_defense", "speed"]
        
        current_date = dt.now().strftime('%m%d%Y')
        csv_filename = f"{prefix}/pokemon_extract_{current_date}.csv"

        with open(csv_filename, "w", newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(pokemon_data)

        return csv_filename

    @task()
    def email_results(filename, email_recipient="cguieb@astronomer.com"):
        """
        #### This function just prints a log statement, but ideally it would send an email of the csv 
        """
        

    pokemon_data = extract_transform()
    load(pokemon_data)


pokemon_etl()
