import requests
import json
import pandas as pd
import csv
from datetime import datetime


def extract():
    all_pokemon_data = []
    url = 'https://pokeapi.co/api/v2/pokemon/'

    # Paginate through each API call and aggregate the results

    # Fetching only the first 151 Pokémon
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

            all_pokemon_data.append(pokemon_info)

        else:
            raise ValueError(f"Failed to fetch data for Pokemon {i}. Status code: {response.status_code}")

    return all_pokemon_data


def load(pokemon_data, prefix="/Users/cguieb/Documents/Astro/artifacts"):
    """
    Given a formatted list of list of Pokemon data, loads the data into a header-formatted csv file.
    """

    headers = ["id", "name", "height", "weight", "type1", "type2", "ability1", "ability2", "total_stats", "hp", "attack", "defense", "special_attack", "special_defense", "speed"]
    
    current_date = datetime.now().strftime('%m%d%Y')
    csv_filename = f"{prefix}/pokemon_extract_{current_date}.csv"

    with open(csv_filename, "w", newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(pokemon_data)


def pokemon_of_the_day():
    """
    """
    pass




fetched_pokemon = extract()
load(fetched_pokemon)
