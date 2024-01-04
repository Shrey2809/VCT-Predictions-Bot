# Drew's server: 
# File system structure: 
#   Contains records           : Records/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.csv
#   Contains winners and points: winner_scores/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.csv

import pandas as pd
import json
import sys
import os

def process_data(guild_id, league, game_type, day):
    folder_path = f'Records/2024/{guild_id}/{league}/{game_type}/'
    if game_type == "IL1" or game_type == "IL2":
        file_name = f'{folder_path}WEEK{day}.csv'
        json_file_name = f'winner_scores/2024/{guild_id}/{league}/{game_type}/WEEK{day}.json'
        
    else:
        file_name = f'{folder_path}DAY{day}.csv'
        json_file_name = f'winner_scores/2024/{guild_id}/{league}/{game_type}/DAY{day}.json'
    
    
    print(f"\nProcessing file: {file_name}")
    
    if not os.path.exists(file_name):
        print(f"File {file_name} does not exist.")
        return

    # Read the CSV file into a DataFrame
    data = pd.read_csv(file_name)

    # Drop duplicates, keeping only the last occurrence of each unique ID
    data.drop_duplicates(subset='MatchIDs', keep='last', inplace=True)

    # Create a dictionary to store the scores for each name
    scores = {}
    score_for_game = {}
    winners_for_game = {}
      
    with open(json_file_name, 'r') as json_file:
        winner_scores = json.load(json_file)
        score_for_game = winner_scores['scores']
        winners_for_game = winner_scores['winners']

    # Calculate the score for all names
    names = data.columns
    for name in names:
        if name not in scores:
            scores[name] = 0
        for index, row in data.iterrows():
            if row[name] == winners_for_game[row['MatchIDs']]:
                scores[name] += score_for_game[row['MatchIDs']]

    # Clean data and store to JSON file
    keys_to_clear = ["MatchIDs", "VCT Predictions Bot"]
    for key in keys_to_clear:
        scores.pop(key, None)

    json_file_name = f"Scores/{game_type}.json"
    existing_data = {}

    # Check if the file exists
    if not os.path.exists(json_file_name):
        with open(json_file_name, 'w') as new_file:
            json.dump({}, new_file)  # Write an empty dictionary

    # Read the existing JSON data from the file
    try:
        with open(json_file_name, 'r') as json_file:
            existing_data = json.load(json_file)
    except Exception as e:
        pass
    

    # Add the new element to the dictionary
    if game_type == "IL1" or game_type == "IL2":
        if league not in existing_data:
            existing_data[league] = {}
        existing_data[league][f"WEEK{day}"] = scores
    else:
        if league not in existing_data:
            existing_data[league] = {}
        existing_data[league][f"DAY{day}"] = scores


    # Write the updated dictionary back to the JSON file
    with open(json_file_name, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)

    print(f"\nData for {file_name} has been written to {json_file_name}")

try:
    # Extract parameters from command line arguments
    if len(sys.argv) >= 5:
        guild_id = sys.argv[1]
        league = sys.argv[2]
        game_type = sys.argv[3]
        day = sys.argv[4]
    else:
        # If guild_id is not provided, use a default value
        guild_id = "963095570124251136"
        league = sys.argv[1]
        game_type = sys.argv[2]
        day = sys.argv[3]
    
    process_data(guild_id, league, game_type, day)
except Exception as e:
    print(f"\nProcess failed: {type(e).__name__}")
