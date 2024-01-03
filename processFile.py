import pandas as pd
import json
import sys 

try:
    fname = sys.argv[1]
    print ("\nEntering file processing")
    # Read the CSV file into a DataFrame
    fname = sys.argv[1]
    data = pd.read_csv(f'Records/{fname}.csv')

    # Drop duplicates, keeping only the last occurrence of each unique ID
    data.drop_duplicates(subset='MatchIDs', keep='last', inplace=True)

    # Create a dictionary to store the scores for each name
    scores           = {}
    score_for_game   = {}
    winners_for_game = {}
    with open(f'winner_scores/{fname}.json', 'r') as json_file:
        winner_scores    = json.load(json_file)
        score_for_game   = winner_scores['scores']
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
    keys_to_clear = ["MatchIDs", "Blast R6 Predictions Bot"]
    for key in keys_to_clear:
        scores.pop(key, None)

    json_file_name = f"Records/KickOffTournament.json"
    existing_data = {}
    # Read the existing JSON data from the file
    try: 
        with open(json_file_name, 'r') as json_file:
            existing_data = json.load(json_file)
    except Exception as e:
        pass

    # Add the new element "DAY2" to the dictionary
    existing_data[fname] = scores

    # Write the updated dictionary back to the JSON file
    with open(json_file_name, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)

    print(f"\nData for {fname} has been written to {json_file_name}")
except Exception as e:
    print(f"\nProcess failed: {type(e).__name__}")
