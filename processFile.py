import csv
import json
import sqlite3
import pandas as pd
import sys

# Get old score from database and if row doesn't exist, enter data into the table
def get_old_score(cursor, table_name, league, user, user_id):
    cursor.execute(f'SELECT {league} FROM {table_name} WHERE user_id = ? and user_name = ?', (user_id, user))
    old_score = cursor.fetchone()
    if old_score == None:
        old_score = 0
        cursor.execute(f'INSERT INTO {table_name} (user_id, user_name) VALUES (?, ?)', (user_id, user))
    else: old_score = old_score[0]
    return old_score


def process_data(guild_id, region, game_type, day):
    # Initialize connection to database
    conn = sqlite3.connect(f'VCT_2024_{guild_id}.db')

    cursor = conn.cursor()

    table_name = f"DS_2024_{game_type}"
    column     = region.lower()
    if game_type == "IL1" or game_type == "IL2":
        record_file_name = f'/home/ubuntu/VCT BOT/Records/2024/{guild_id}/{region}/{game_type}/WEEK{day}.csv'
        winner_scores_file_name = f'/home/ubuntu/VCT BOT/winner_scores/2024/{guild_id}/{region}/{game_type}/WEEK{day}.json'
    else:
        record_file_name = f'/home/ubuntu/VCT BOT/Records/2024/{guild_id}/{region}/{game_type}/DAY{day}.csv'
        winner_scores_file_name = f'/home/ubuntu/VCT BOT/winner_scores/2024/{guild_id}/{region}/{game_type}/DAY{day}.json'

    # Read record file and winner scores JSON file
    df = pd.read_csv(record_file_name)
    with open(winner_scores_file_name, 'r') as file:
        winner_scores = json.load(file)


    names = df.columns

    cursor.execute(f'SELECT * FROM DS_2024_FILES_PROCESSED WHERE file_name = ?', (record_file_name,))
    if cursor.fetchone() != None:
        print('File already processed')
        exit()

    user_scores = {}  # Dictionary to store user scores

    names = df.columns
    # Process Record Data
    for name in names:
        if name not in user_scores and name != 'MatchIDs' and name != 'VCT Predictions Bot':
            specific_row = df.loc[df['MatchIDs'] == 'UserIDs']
            user_id = specific_row[name].tolist()[0]
            user_scores[name] = get_old_score(cursor, table_name, column, name, user_id)
        for index, row in df.iterrows():
            if index != 0:
                if row[name] == winner_scores['winners'][row['MatchIDs']]:
                    user_scores[name] += winner_scores['scores'][row['MatchIDs']]
        

    for item in user_scores:
        specific_row = df.loc[df['MatchIDs'] == 'UserIDs']
        user_id = specific_row[item].tolist()[0]
        sql_query = f'UPDATE {table_name} SET {column} = {user_scores[item]} WHERE user_id = {user_id}'
        cursor.execute(sql_query)
        conn.commit()

    cursor.execute("INSERT INTO DS_2024_FILES_PROCESSED (file_name) VALUES (?)", (record_file_name,))
    conn.commit()

    conn.close()


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
        # guild_id = "1042862967072501860"
        league = sys.argv[1]
        game_type = sys.argv[2]
        day = sys.argv[3]

    process_data(guild_id, league, game_type, day)
except Exception as e:
    print(f"\nProcess failed: {type(e).__name__}")