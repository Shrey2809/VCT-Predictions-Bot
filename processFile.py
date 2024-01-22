import csv
import json
import sqlite3
import pandas as pd
import sys

# Get old score from database and if row doesn't exist, enter data into the table
def get_old_score(cursor, league, game_type, user, user_id, playoffs_flag = False):
    if game_type in ["masters", "champions"]:
        if playoffs_flag is True:
            query = f'SELECT {game_type}_{league}_playoffs FROM DS_VCT_2024 WHERE user_id = ? and user_name = ?'
        else:
            query = f'SELECT {game_type}_{league}_groups FROM DS_VCT_2024 WHERE user_id = ? and user_name = ?'
    else:
        query = f'SELECT {game_type}_{league} FROM DS_VCT_2024 WHERE user_id = ? and user_name = ?'
    print(query)
    cursor.execute(query, (user_id, user))
    old_score = cursor.fetchone()
    if old_score == None:
        old_score = 0
        cursor.execute(f'INSERT INTO DS_VCT_2024 (user_id, user_name) VALUES (?, ?)', (user_id, user))
    else: old_score = old_score[0]
    return old_score


def process_data(guild_id, league, game_type, day, playoffs_flag = False):
    # Initialize connection to database
    conn = sqlite3.connect(f'VCT_2024_{guild_id}.db')

    cursor = conn.cursor()

    table_name = f"DS_2024_{game_type}"
    column     = league.lower()
    if game_type == "IL1" or game_type == "IL2":
        record_file_name = f'/home/ubuntu/VCT BOT/Records/2024/{guild_id}/{league}/{game_type}/WEEK{day}.csv'
        winner_scores_file_name = f'/home/ubuntu/VCT BOT/winner_scores/2024/{guild_id}/{league}/{game_type}/WEEK{day}.json'
    else:
        record_file_name = f'/home/ubuntu/VCT BOT/Records/2024/{guild_id}/{league}/{game_type}/DAY{day}.csv'
        winner_scores_file_name = f'/home/ubuntu/VCT BOT/winner_scores/2024/{guild_id}/{league}/{game_type}/DAY{day}.json'

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
            user_scores[name] = get_old_score(cursor, league, game_type, name, user_id, playoffs_flag)
        for index, row in df.iterrows():
            if index != 0:
                if row[name] == winner_scores['winners'][row['MatchIDs']] and winner_scores['record'][row['MatchIDs']] == 'Record':
                    user_scores[name] += winner_scores['scores'][row['MatchIDs']]
        

    for item in user_scores:
        specific_row = df.loc[df['MatchIDs'] == 'UserIDs']
        user_id = specific_row[item].tolist()[0]
        if game_type in ["masters", "champions"]:
            if playoffs_flag is True:
                query = f'UPDATE DS_VCT_2024 SET {game_type}_{league}_playoffs = {user_scores[item]} WHERE user_id = {user_id}' 
            else:
                query = f'UPDATE DS_VCT_2024 SET {game_type}_{league}_groups = {user_scores[item]} WHERE user_id = {user_id}'
        else:
            query = f'UPDATE DS_VCT_2024 SET {game_type}_{league} = {user_scores[item]} WHERE user_id = {user_id}'
        cursor.execute(query)
        conn.commit()

    cursor.execute("INSERT INTO DS_2024_FILES_PROCESSED (file_name) VALUES (?)", (record_file_name,))
    conn.commit()

    conn.close()


try:
    if len(sys.argv) >= 6:
        guild_id = sys.argv[1]
        league = sys.argv[2]
        game_type = sys.argv[3]
        day = sys.argv[4]

        # Check if playoffs flag is provided
        if len(sys.argv) > 5 and sys.argv[5] == "--playoffs":
            playoffs_flag = True
        else:
            playoffs_flag = False

    else:
        # If guild_id is not provided, use a default value
        guild_id = "963095570124251136"
        league = sys.argv[1]
        game_type = sys.argv[2]
        day = sys.argv[3]
        # Check if playoffs flag is provided
        if len(sys.argv) > 4 and sys.argv[4] == "--playoffs":
            playoffs_flag = True
        else:
            
            playoffs_flag = False

    print(f"Processing data for {league} {game_type} {day} {playoffs_flag}")
    process_data(guild_id, league, game_type, day, playoffs_flag)

except Exception as e:
    print(f"\nProcess failed: {type(e).__name__}")

# UNIT TEST CODE
# conn          = sqlite3.connect(f'VCT_2024_1042862967072501860.db')
# cursor        = conn.cursor()
# league        = "shanghai" 
# game_type     = "masters"
# user          = "axon319"
# user_id       = 400713084232138755
# playoffs_flag = True
# print(get_old_score(cursor, league, game_type, user, user_id, playoffs_flag))