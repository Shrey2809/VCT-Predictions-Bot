import discord
from discord.ext import commands
import logging
import aiohttp
import json
import pandas as pd
from tabulate import tabulate
import random
import csv
import re
import os
import sqlite3

class vctBotBackend(commands.AutoShardedBot):
    # Initialize the bot
    def __init__(self, config):
        # prefixes = ["+", "poll:", "Poll:", "POLL:"]
        self.emojiLetters = ["<:VCTA:1191943585277423706>", "<:VCTB:1191943586749632552>"]

        # Opening JSON file
        with open('etc/emotes.json') as json_file:
            self.emojiListFront = json.load(json_file)

        self.team1  = []
        self.team2  = []
        self.points = []

        super().__init__(command_prefix="+poll", status=discord.Status.online, intents=discord.Intents.all())
        self.config = config
        self.shard_count = self.config["shards"]["count"]
        shard_ids_list = []

        # create list of shard ids
        for i in range(self.config["shards"]["first_shard_id"], self.config["shards"]["last_shard_id"] + 1):
            shard_ids_list.append(i)
        self.shard_ids = tuple(shard_ids_list)

        self.remove_command("help")
        self.messages = []

        with open('etc/emotes.json') as json_file:
            my_map = json.load(json_file)

        self.emojiListBack = {v: k for k, v in my_map.items()}

        self.superSet = set()
        self.teamUsersA = []
        self.teamUsersB = []
        self.outputList = []

        self.admin_users = ["axon319", "blkout", "captaincow285", "electriccmars", "janesgg", "cdaps", "drewspark"]

        self.leagues = {"CHINA": 0xFC2659,
                            "PACIFIC": 0x01d2d7,
                            "AMERICAS": 0xff570c,
                            "EMEA": 0xdc3030,
                            "MASTERS": 0x6f4acc,
                            "CHAMPIONS": 0xc5b174}


        super().__init__(command_prefix="+score", status=discord.Status.online, intents=discord.Intents.all())
        self.columns_to_show = ["Rank", "Total"]  # "Playins", "Playoffs", "Finals"
        # Configure the logger
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(levelname)s] %(message)s',
            filename='VCT2024.log',  # Specify the path to your log file
            filemode='a'  # Use 'a' to append to the file, 'w' to overwrite
        )
        self.fname = ''
        self.logger = logging.getLogger(__name__)
        
    # Parse the poll string
    def parse_poll_string(self, input_string):
        poll_match = re.match(r"\+poll\s+(\w+)\s+(\w+)\s+(\d+)", input_string)
        record_match = re.match(r"\+record\s+(\w+)\s+(\w+)\s+(\d+)", input_string)
        build_match = re.match(r"\+build\s+(\w+)\s+(\w+)\s+(\d+)", input_string)
        load_match  = re.match(r"\+load\s+(\w+)\s+(\w+)\s+(\d+)", input_string)

        if poll_match:
            league = poll_match.group(1)
            poll_type = poll_match.group(2)
            day = int(poll_match.group(3))
            result = {"Command": "poll", "League": league, "Type": poll_type, "Day": day}
            return result
        elif record_match:
            league = record_match.group(1)
            record_type = record_match.group(2)
            day = int(record_match.group(3))
            result = {"Command": "record", "League": league, "Type": record_type, "Day": day}
            return result
        elif build_match:
            league = build_match.group(1)
            build_type = build_match.group(2)
            day = int(build_match.group(3))
            result = {"Command": "build", "League": league, "Type": build_type, "Day": day}
            return result
        elif load_match:
            league = load_match.group(1)
            load_type = load_match.group(2)
            day = int(load_match.group(3))
            result = {"Command": "load", "League": league, "Type": load_type, "Day": day}
            return result
        else:
            return None

    # Parse the rank string
    def parse_rank_string(self, input_string):
        input_string = input_string.lower()
        all             = re.match(r"\+rank\s+all", input_string)
        league_or_type  = re.match(r"\+rank\s+(\w+)", input_string)
        league_and_type = re.match(r"\+rank\s+(\w+)\s+(\w+)", input_string)
        latest          = re.match(r"\+rank", input_string)

        if all:
            return {"Command": "rank", "Type": "all"}
        
        elif league_and_type:
            if (league_and_type.group(1) in ["china", "pacific", "americas", "emea", "madrid", "korea", "shanghai"] and 
                    league_and_type.group(2) in ["il1", "il2", "kickoff", "masters", "champions"]):
                return {"Command": "rank", "Type": "specific", "league": league_and_type.group(1), "type": league_and_type.group(2)}
            elif (league_and_type.group(2) in ["china", "pacific", "americas", "emea", "madrid", "korea", "shanghai"] and 
                    league_and_type.group(1) in ["il1", "il2", "kickoff", "masters", "champions"]):
                return {"Command": "rank", "Type": "specific", "league": league_and_type.group(2), "type": league_and_type.group(1)}
            else:
                return {"Command": "rank", "Type": "404"}
            
        elif league_or_type:
            if league_or_type.group(1) in ["china", "pacific", "americas", "emea", "madrid", "korea", "shanghai"]:
                return {"Command": "rank", "Type": "league", "league": league_or_type.group(1)}
            elif league_or_type.group(1) in ["il1", "il2", "kickoff", "masters", "champions"]:
                return {"Command": "rank", "Type": "type", "type": league_or_type.group(1)}
            else:
                return {"Command": "rank", "Type": "404"}
            
        elif latest:
            return {"Command": "rank", "Type": "latest"}
        else:
            return {"Command": "rank", "Type": "404"}

    # Load all the teams into the team1 and team2 lists
    def get_team_lists(self, title):
        fileName = "Games/" + title + ".csv"
        file = open(fileName, "r")
        i = 1
        for row in file:
            r = row.strip().split(',')
            if len(r) >= 2:
                team1_name = r[0]
                team2_name = r[1]
                points = int(r[2].strip()) if len(r) > 2 and r[2].strip() else 1
                
                self.team1.append(team1_name)
                self.team2.append(team2_name)
                self.points.append(points)

                self.logger.info(f"Team 1: {team1_name}, Team 2: {team2_name}, Points: {points}")
            else:
                self.logger.warning(f"Issue with row {i} in {fileName}. Skipping due to incomplete data.")
            i += 1
        file.close()

    # Get the emote for the team
    def get_team_emote(self, team, i):
        try:
            emote = self.emojiListFront[team]
        except:
            emote = self.emojiLetters[i]
        return emote

    # Tabulate data for the embed to send to Discord
    def tabulate_df(self, df, userInfo, item):
        df = df[["Rank", "Points"]]
        table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
        embed = discord.Embed(title=f"{userInfo}'s rank for {item.upper()}", color=self.generate_random_color())
        embed.add_field(name='\u200b', value=f'```\n{table}\n```')
        return embed

    # Get points and rank for a specific user for a league
    def get_league_sum(self, conn, league, username=None, user_id=None):
        # Create a cursor object to interact with the database
        cursor = conn.cursor()
        
        if league in ["china", "pacific", "americas", "emea"]:
            query = f"""
                    SELECT user_name, total_{league}, rank_{league} 
                    FROM (SELECT user_name, user_id, total_{league},
                            (SELECT COUNT(DISTINCT rank_val) + 1
                                FROM (SELECT COALESCE(SUM({league}), 0) AS total_{league}, DENSE_RANK() OVER (ORDER BY COALESCE(SUM({league}), 0) DESC) AS rank_val
                                    FROM (
                                        SELECT user_name, user_id, {league} FROM DS_2024_KICKOFF
                                        UNION ALL
                                        SELECT user_name, user_id, {league} FROM DS_2024_IL1
                                        UNION ALL
                                        SELECT user_name, user_id, {league} FROM DS_2024_IL2
                                    ) AS all_{league}
                                    GROUP BY user_name, user_id  -- Added user_id to the GROUP BY clause
                                ) AS ranks
                                WHERE ranks.total_{league} > ua.total_{league} OR (ranks.total_{league} = ua.total_{league} AND ranks.rank_val < ua.rank_val)
                            ) AS rank_{league}
                        FROM (
                            SELECT user_name, user_id, COALESCE(SUM({league}), 0) AS total_{league}, DENSE_RANK() OVER (ORDER BY COALESCE(SUM({league}), 0) DESC) AS rank_val
                            FROM (
                                SELECT user_name, user_id, {league} FROM DS_2024_KICKOFF
                                UNION ALL
                                SELECT user_name, user_id, {league} FROM DS_2024_IL1
                                UNION ALL
                                SELECT user_name, user_id, {league} FROM DS_2024_IL2
                            ) AS all_{league}
                            GROUP BY user_name, user_id  -- Added user_id to the GROUP BY clause
                        ) AS ua
                    ) 
                    WHERE user_name = '{username}' or user_id = {user_id};
            """
        elif league in ["madrid", "shanghai"]:
            query = f"""
                    SELECT 
                        user_name, 
                        total_{league}, 
                        rank_{league}
                    FROM (  
                        SELECT 
                            user_name, 
                            user_id,
                            total_{league},
                            (
                                SELECT COUNT(DISTINCT rank_val) + 1
                                FROM (
                                    SELECT 
                                        COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                                        DENSE_RANK() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_val
                                    FROM (
                                        SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_MASTERS
                                    ) AS all_{league}
                                    GROUP BY user_name, user_id 
                                ) AS ranks
                                WHERE ranks.total_{league} > ua.total_{league} OR (ranks.total_{league} = ua.total_{league} AND ranks.rank_val < ua.rank_val)
                            ) AS rank_{league}
                        FROM (
                            SELECT 
                                user_name, 
                                user_id, 
                                COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                                DENSE_RANK() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_val
                            FROM (
                                SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_MASTERS
                            ) AS all_{league}
                            GROUP BY user_name, user_id 
                        ) AS ua
                    ) 
                    WHERE user_name = '{username}' or user_id = {user_id};
            """
        elif league in ["korea"]:
            query = f"""
                    SELECT 
                        user_name, 
                        total_{league}, 
                        rank_{league}
                    FROM (  
                        SELECT 
                            user_name, 
                            user_id,
                            total_{league},
                            (
                                SELECT COUNT(DISTINCT rank_val) + 1
                                FROM (
                                    SELECT 
                                        COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                                        DENSE_RANK() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_val
                                    FROM (
                                        SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_CHAMPIONS
                                    ) AS all_{league}
                                    GROUP BY user_name, user_id 
                                ) AS ranks
                                WHERE ranks.total_{league} > ua.total_{league} OR (ranks.total_{league} = ua.total_{league} AND ranks.rank_val < ua.rank_val)
                            ) AS rank_{league}
                        FROM (
                            SELECT 
                                user_name, 
                                user_id, 
                                COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                                DENSE_RANK() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_val
                            FROM (
                                SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_CHAMPIONS
                            ) AS all_{league}
                            GROUP BY user_name, user_id 
                        ) AS ua
                    ) 
                    WHERE user_name = '{username}' or user_id = {user_id};  
                    
            """
        
        # Execute the query with the provided username or user_id
        cursor.execute(query)
        
        # Fetch the result
        result = cursor.fetchall()
        
        # Close the cursor and the connection
        cursor.close()
        conn.close()
        
        # Return the result as a DataFrame
        df = pd.DataFrame(result, columns=['user_name', f'total_{league}', f'rank_{league}'])
        df = df.rename(columns={f'total_{league}': 'Points', f'rank_{league}': 'Rank'})
        return df

    # Get points and rank for a specific user for a region + type
    def get_specific_sum(self, conn, league, type, username=None, user_id=None):
        cursor = conn.cursor()

        if type in ["il1", "il2", "kickoff"]:
            query = f"""
            SELECT  user_id,
                    user_name,
                    total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    (
                        SELECT COUNT(DISTINCT total_sum) + 1
                        FROM (
                            SELECT 
                                SUM({league}) AS total_sum
                            FROM DS_2024_{type.upper()}
                            GROUP BY user_name
                        ) AS ranks
                        WHERE total_sum > sums.total_sum
                    ) AS dense_rank
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM({league}) AS total_sum
                    FROM DS_2024_{type.upper()}
                    GROUP BY user_id, user_name
                ) AS sums
            ) 
            WHERE user_id = {user_id} or user_name = "{username}"
            """
        elif type in ["madrid", "shanghai"]:
            query = f"""
            SELECT  user_id,
                    user_name,
                    total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    (
                        SELECT COUNT(DISTINCT total_sum) + 1
                        FROM (
                            SELECT 
                                SUM({league}_groups + {league}_playoffs) AS total_sum
                            FROM DS_2024_MASTERS
                            GROUP BY user_name
                        ) AS ranks
                        WHERE total_sum > sums.total_sum
                    ) AS dense_rank
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM({league}_groups + {league}_playoffs) AS total_sum
                    FROM DS_2024_MASTERS
                    GROUP BY user_id, user_name
                ) AS sums
            ) 
            WHERE user_id = {user_id} or user_name = "{username}"
            """
        elif type in ["korea"]:
            query = f"""
            SELECT  user_id,
                    user_name,
                    total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type} 
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    (
                        SELECT COUNT(DISTINCT total_sum) + 1
                        FROM (
                            SELECT 
                                SUM({league}_groups + {league}_playoffs) AS total_sum
                            FROM DS_2024_CHAMPIONS
                            GROUP BY user_name
                        ) AS ranks
                        WHERE total_sum > sums.total_sum
                    ) AS dense_rank
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM({league}_groups + {league}_playoffs) AS total_sum
                    FROM DS_2024_CHAMPIONS
                    GROUP BY user_id, user_name
                ) AS sums
            ) 
            WHERE user_id = {user_id} or user_name = "{username}"
            """
        
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        
        df = pd.DataFrame(result, columns=['user_id', 'user_name', f'total_{league}_{type}', f'rank_{league}_{type}'])
        df = df.rename(columns={f'total_{league}_{type}': 'Points', f'rank_{league}_{type}': 'Rank'})
        return df

    # Get points and rank for a specific user for a type
    def get_type_sum(self, conn, type, username=None, user_id=None):
        cursor = conn.cursor()

        if type in ["il1", "il2", "kickoff"]:
            query = f"""
            SELECT  user_id,
                    user_name,
                    total_sum as total_{type}, dense_rank as rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    (
                        SELECT COUNT(DISTINCT total_sum) + 1
                        FROM (
                            SELECT 
                                SUM(emea) + SUM(china) + SUM(pacific) + SUM(americas)  AS total_sum
                            FROM DS_2024_IL1
                            GROUP BY user_name
                        ) AS ranks
                        WHERE total_sum > sums.total_sum
                    ) AS dense_rank
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(emea) + SUM(china) + SUM(pacific) + SUM(americas) AS total_sum
                    FROM DS_2024_IL1
                    GROUP BY user_id, user_name
                ) AS sums
            ) 
            WHERE user_id = {user_id} or user_name = "{username}"
            """
        elif type in ["masters"]:
            query = f"""
            SELECT  user_id,
                    user_name,
                    total_sum as total_{type}, dense_rank as rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    (
                        SELECT COUNT(DISTINCT total_sum) + 1
                        FROM (
                            SELECT 
                                SUM(shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs)  AS total_sum
                            FROM DS_2024_MASTERS
                            GROUP BY user_name
                        ) AS ranks
                        WHERE total_sum > sums.total_sum
                    ) AS dense_rank
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs) AS total_sum
                    FROM DS_2024_MASTERS
                    GROUP BY user_id, user_name
                ) AS sums
            ) 
            WHERE user_id = {user_id} or user_name = "{username}"
            """
        elif type in ["champions"]:
            query = f"""
            SELECT  user_id,
                    user_name,
                    total_sum as total_{type}, dense_rank as rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    (
                        SELECT COUNT(DISTINCT total_sum) + 1
                        FROM (
                            SELECT 
                                SUM({type}_groups + {type}_playoffs)  AS total_sum
                            FROM DS_2024_CHAMPIONS
                            GROUP BY user_name
                        ) AS ranks
                        WHERE total_sum > sums.total_sum
                    ) AS dense_rank
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM({type}_groups + {type}_playoffs) AS total_sum
                    FROM DS_2024_CHAMPIONS
                    GROUP BY user_id, user_name
                ) AS sums
            ) 
            WHERE user_id = {user_id} or user_name = "{username}"
            """
        
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        df = pd.DataFrame(result, columns=['user_id', 'user_name', f'total_{type}', f'rank_{type}'])
        df = df.rename(columns={f'total_{type}': 'Points', f'rank_{type}': 'Rank'})
        return df

    # Get points and rank for a specific user for all year long
    def get_all_sum(self, conn, username=None,user_id=None):
        cursor = conn.cursor()

        query = f"""
                SELECT 
                    user_name, 
                    user_id, 
                    overall_total,
                    (
                        SELECT COUNT(DISTINCT overall_total) + 1
                        FROM (
                            SELECT 
                                SUM(total_sum) AS overall_total
                            FROM (
                                SELECT user_name, user_id, americas + emea + china + pacific AS total_sum
                                FROM DS_2024_KICKOFF

                                UNION ALL

                                SELECT user_name, user_id, americas + emea + china + pacific AS total_sum
                                FROM DS_2024_IL1

                                UNION ALL

                                SELECT user_name, user_id, americas + emea + china + pacific AS total_sum
                                FROM DS_2024_IL2

                                UNION ALL

                                SELECT user_name, user_id, shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs AS total_sum
                                FROM DS_2024_MASTERS

                                UNION ALL

                                SELECT user_name, user_id, korea_groups + korea_playoffs AS total_sum
                                FROM DS_2024_CHAMPIONS
                            ) AS all_tables
                            GROUP BY user_name, user_id
                        ) AS ranks
                        WHERE overall_total > sums.overall_total
                    ) AS dense_rank
                FROM (
                    SELECT 
                        user_name, 
                        user_id, 
                        SUM(total_sum) AS overall_total
                    FROM (
                        SELECT 
                            user_name, 
                            user_id, 
                            americas + emea + china + pacific AS total_sum
                        FROM DS_2024_KICKOFF

                        UNION ALL

                        SELECT 
                            user_name, 
                            user_id, 
                            americas + emea + china + pacific AS total_sum
                        FROM DS_2024_IL1

                        UNION ALL

                        SELECT 
                            user_name, 
                            user_id, 
                            americas + emea + china + pacific AS total_sum
                        FROM DS_2024_IL2

                        UNION ALL

                        SELECT 
                            user_name, 
                            user_id, 
                            shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs AS total_sum
                        FROM DS_2024_MASTERS

                        UNION ALL

                        SELECT 
                            user_name, 
                            user_id, 
                            korea_groups + korea_playoffs AS total_sum
                        FROM DS_2024_CHAMPIONS
                    ) AS all_tables
                    GROUP BY user_name, user_id
                ) AS sums
                WHERE user_name ="{username}" or user_id = {user_id}
        """

        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        df = pd.DataFrame(result, columns=['user_name', 'user_id', 'overall_total', 'dense_rank'])
        df = df.rename(columns={'overall_total': 'Points', 'dense_rank': 'Rank'})

        return df

    # Generate a random 24-bit color code
    def generate_random_color(self):
        color = random.randint(0, 0xFFFFFF)  # 0x000000 to 0xFFFFFF (0 to 16777215 in decimal)
        return color
    
    # Message displayed when bot is started
    async def on_ready(self):
        self.http_session = aiohttp.ClientSession()
        print("-------------")
        print("|VCT Poll Bot|")
        print("-------------")
        self.logger.info("-------------")
        self.logger.info("|VCT Poll Bot|")
        self.logger.info("-------------")

    # Process a message
    async def on_message(self, message):
        #----------------------------------------------------------------------------------------------------
        # Admin access API
        if message.author.name in self.admin_users:
            # Create polls from a file  <+poll LEAGUE DAY TYPE}>
            if message.content.startswith("+poll"):
                messageContent = message.clean_content
                server_id = message.guild.id if message.guild else None
        
                titleDict = self.parse_poll_string(messageContent)
                pollColor = self.leagues[titleDict["League"]]
                if titleDict["Type"] == "IL1" or titleDict["Type"] == "IL2":
                    title = f'''2024/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}'''
                else:
                    title = f'''2024/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}'''

                league    = self.emojiListFront[titleDict["League"]]
                day       = titleDict["Day"]
                winner    = {}
                score     = {}

                self.get_team_lists(title)
                self.maxGames = len(self.team1)
                j = 1
                while j <= self.maxGames:
                    pollMessage = ""
                    options = []
                    i = 0
                    options.append(self.team1[j-1])
                    options.append(self.team2[j-1])
                    teamA = self.team1[j-1]
                    teamB = self.team2[j-1]
                    emoteA = self.get_team_emote(teamA, 0)
                    emoteB = self.get_team_emote(teamB, 1)

                    for choice in options:
                        if not options[i] == "":
                            if len(options) > 21:
                                return
                            elif not i == len(options):
                                if i %2 == 0:
                                    pollMessage = pollMessage + "\n\n" + emoteA + " " + choice
                                else :
                                    pollMessage = pollMessage + "\n\n" + emoteB + " " + choice
                        i += 1

                    boxTitle = f"**{league} D{day} G{j}: {self.team1[j-1]} vs {self.team2[j-1]} (Points: {self.points[j-1]})**"
                    e = discord.Embed(title=boxTitle, description=pollMessage, colour=pollColor)
                    pollMessage = await message.channel.send(embed=e)
                    self.messages.append(pollMessage)

                    winner[f"MATCH {j}"] = f"{self.team1[j-1]}/{self.team2[j-1]}"
                    score[f"MATCH {j}"]  = self.points[j-1]

                    i = 0
                    final_options = [] 
                    j += 1

                    for choice in options:
                        if not i == len(options) and not options[i] == "":
                            if i % 2 == 0:
                                final_options.append(choice)
                                await pollMessage.add_reaction(emoteA)
                            else:
                                final_options.append(choice)
                                await pollMessage.add_reaction(emoteB)
                            i += 1
                
                if titleDict["Type"] == "IL1" or titleDict["Type"] == "IL2":
                    out_title = f'''2024/{server_id}/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}'''
                else:
                    out_title = f'''2024/{server_id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}'''

                directory = f'''/2024/{server_id}/{titleDict["League"]}/{titleDict["Type"]}'''
                
                msgIds = []

                os.makedirs(f"IDs/{directory}", exist_ok=True)
                os.makedirs(f"winner_scores/{directory}", exist_ok=True)
                os.makedirs(f"Records/{directory}", exist_ok=True)
                with open(f"IDs/{out_title}.txt", 'w') as id_file:
                    for msg in self.messages:
                        msgIds.append(msg.id)
                        id_file.write(f"{msg.id}\n")

                winner_score = {}
                winner_score["scores"] = score
                winner_score["winners"] = winner
                with open(f'winner_scores/{out_title}.json', 'w') as json_file:
                    json.dump(winner_score, json_file, indent=4)

                self.messages = []
                self.team1    = []
                self.team2    = [] 
                self.points   = []            

            # Create a top 10 list of standings <+leaderboard>
            if message.content.startswith("+leaderboard"):
                self.logger.info("Leaderboard requested")
                dataframe   = self.createDataframe()
                tempdf = dataframe.head(10)[["Rank", "Name", "Total"]]
                table = tabulate(tempdf, headers='keys', tablefmt="fancy_outline", showindex="never")
                embed = discord.Embed(title='Leaderboard: Top 10', color=self.generate_random_color())
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)

            # Get a specific user's stats <+score NAME>
            if message.content.startswith("+score"):
                nameToCheck = message.content[len("+score "):]
                self.logger.info(f"Score requested for {nameToCheck}")
                tempdf = self.get_df_based_on_name(nameToCheck)
                tempdf = tempdf[self.columns_to_show]
                table = tabulate(tempdf, headers='keys', tablefmt="simple_outline", showindex="never")
                embed = discord.Embed(title=f"{nameToCheck}'s Rank", color=self.generate_random_color())
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)
            
            # Check file names <+check>
            if message.content.startswith("+check"):
                await message.channel.send(f'File name loaded: {self.fname}')

            # Load a specific file for the list of IDs <+load {Title}>
            if message.content.startswith("+load"):
                messageContent = message.clean_content
                titleDict = self.parse_poll_string(messageContent)
                if titleDict["Type"] == "IL1" or titleDict["Type"] == "IL2":
                    self.fname = f'''Records/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}.csv'''
                    id_fname   = f'''IDs/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}.txt'''
                else: 
                    self.fname = f'''Records/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.csv'''
                    id_fname   = f'''IDs/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.txt'''
                
                with open(id_fname)  as IDs:
                    self.messageIDs = [int(line.strip()) for line in IDs]
                self.logger.info(f"Records and ID files loaded: {self.fname}")
                print(f"Records and ID files loaded: {self.fname}")

            # Setting winner for game <+setwinner {DayNumber} {GameNumber} {TeamName} {Score}>
            if message.content.startswith("+setwinner"):
                pattern = r"\+setwinner (\d+) (\d+) (\w+) (\d+)"

                # Use re.match to search for the pattern in the incoming message
                match = re.match(pattern, message.clean_content)

                if match:
                    day_number = int(match.group(1))  # The first captured group is DayNumber
                    game_number = int(match.group(2))  # The second captured group is GameNumber
                    team_name = match.group(3)        # The third captured group is TeamName
                    score = int(match.group(4))        # The fourth captured group is Score

                    # Now you can use these variables
                    with open(f"winner_scores/DAY{day_number}.json") as f:
                        data = json.load(f)
                    data["scores"][f"MATCH {game_number}"] = score
                    data["winners"][f"MATCH {game_number}"] = team_name

                    with open(f"winner_scores/DAY{day_number}.json", "w") as f:
                        json.dump(data, f, indent=4)
                    self.logger.info(f"Score for match {game_number} on day {day_number} set to {score} for {team_name}")
                else:
                    print("Invalid command format")
                    self.logger.info("Invalid command format")
                
            # Exit the bot <+end> or <+exit>
            if message.content.startswith("+end") or message.content.startswith("+exit"):
                self.logger.info("Bot's going offline!")
                await message.channel.send("Bot's going offline!")
                await self.close()

        # ----------------------------------------------------------------------------------------------------
        # General user access API
        # Build the base file with the names of all the users <+build {Title}>
        if message.content.startswith("+build"):
            messageContent = message.clean_content
            titleDict = self.parse_poll_string(messageContent)
            if titleDict["Type"] == "IL1" or titleDict["Type"] == "IL2":
                self.fname = f'''Records/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}.csv'''
                id_fname   = f'''IDs/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}.txt'''
            else: 
                self.fname = f'''Records/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.csv'''
                id_fname   = f'''IDs/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.txt'''

            with open(id_fname)  as IDs:
                self.messageIDs = [int(line.strip()) for line in IDs]

            user_id_name_map = {} 
            for messageID in self.messageIDs:
                msg = await message.channel.fetch_message(messageID)
                for r in msg.reactions:
                    users = set()
                    
                    async for user in r.users():
                        users.add(user.name)
                        user_id_name_map[user.name] = user.id  

                    self.superSet = self.superSet.union(users)

            setOfUsers = list(self.superSet)
    
            print(user_id_name_map)
            user_ids = [user_id_name_map[user_name] for user_name in setOfUsers if user_name != 'MatchIDs']

            setOfUsers.append('MatchIDs')
            user_ids.append('UserIDs')

            with open(self.fname, 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(setOfUsers)
                writer.writerow(user_ids)
                
            dictOfFile = self.parse_poll_string(messageContent)
            print (f"File and list created: {dictOfFile}")
            self.logger.info(f"File and list created: {dictOfFile}")
    
        # Close a specific game <+close GameNumber>
        if message.content.startswith("+close"):
            task  = message.content[len("+close "):]
            i     = int(task)
            print (self.messageIDs[i-1])
            messageID = self.messageIDs[i-1]
            self.logger.info(f"Game {i}")
            msg = await message.channel.fetch_message(messageID)
            j=0
            
            # Get list of users from a message
            for reaction in msg.reactions:
                reactionStr = str(reaction)
                
                # If j == 0, then it's the first team, if j == 1, then it's the second team
                if j == 0:
                    if reactionStr in self.emojiListBack.keys():
                        teamAName = self.emojiListBack[reactionStr]
                    else:
                        teamAName = "A"
                elif j == 1:
                    if reactionStr in self.emojiListBack.keys():
                        teamBName = self.emojiListBack[reactionStr]
                    else:
                        teamBName = "B"

                # get the list of users for a team
                users = set()
                user_id_name_map = {} 
                async for user in reaction.users():
                    users.add(user.name)
                    user_id_name_map[user.name] = user.id  

                # Create team A list and create team B list
                if j == 0:
                    teamAList = list(users)
                    teamCount = len(teamAList)
                elif j == 1:
                    teamBList = list(users)
                    teamCount = len(teamBList)

                teamCount = teamCount - 1
                print (reactionStr + ' has count: ' + str(teamCount))
                self.logger.info(f"{reactionStr} has count: {teamCount}")
                j+=1

            # read the CSV file and get the user names from CSV
            df = pd.read_csv(self.fname)
            specific_matchID = "MATCH " + str(i)

            # Filter the DataFrame to select rows with the specific matchID
            match_rows = df[df["MatchIDs"] == specific_matchID]

            # Add the row if it doesn't exist
            if match_rows.empty:
                new_row = {"MatchIDs": specific_matchID}
                df.loc[len(df)] = new_row
            
            # For each name in name of columns if column is not of MatchIDs          
            for column in df.columns:
                if column != "MatchIDs":
                    if column in teamBList and column not in teamAList:
                        df[column] = df[column].astype(object)
                        df.loc[df['MatchIDs'] == specific_matchID, column] = teamBName
                    elif column in teamAList and column not in teamBList:
                        df[column] = df[column].astype(object)
                        df.loc[df['MatchIDs'] == specific_matchID, column] = teamAName
                    else:
                        df[column] = df[column].astype(object)
                        df.loc[df['MatchIDs'] == specific_matchID, column] = "N/A"
            
            # If a user doesn't exist in original list, they're added to the list with N/A and then their pick is updated for the match
            for user in teamAList:
                if user not in df.columns:
                    df[user] = "N/A"
                    df[user] = df[user].astype(object)
                    df.loc[df['MatchIDs'] == "UserIDs", user] = user_id_name_map[user]
                    df.loc[df['MatchIDs'] == specific_matchID, user] = teamAName

            for user in teamBList:
                if user not in df.columns:
                    df[user] = "N/A"
                    df[user] = df[user].astype(object)
                    df.loc[df['MatchIDs'] == "UserIDs", user] = user_id_name_map[user]
                    df.loc[df['MatchIDs'] == specific_matchID, user] = teamBName

            # Update the original DataFrame with the modified match rows
            df.to_csv(self.fname, index=False)

            print ("Game is now closed")
            self.logger.info("Game is now closed")

        # Record all games in a set of polls from the file <+record {Title}>
        if message.content.startswith("+record"):
            messageContent = message.clean_content
            titleDict = self.parse_poll_string(messageContent)
            if titleDict["Type"] == "IL1" or titleDict["Type"] == "IL2":
                self.fname = f'''Records/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}.csv'''
                id_fname   = f'''IDs/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/WEEK{titleDict["Day"]}.txt'''
            else: 
                self.fname = f'''Records/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.csv'''
                id_fname   = f'''IDs/2024/{message.guild.id}/{titleDict["League"]}/{titleDict["Type"]}/DAY{titleDict["Day"]}.txt'''

            with open(id_fname)  as IDs:
                self.messageIDs = [int(line.strip()) for line in IDs]

            user_id_name_map = {} 
            for messageID in self.messageIDs:
                msg = await message.channel.fetch_message(messageID)
                for r in msg.reactions:
                    users = set()
                    
                    async for user in r.users():
                        users.add(user.name)
                        user_id_name_map[user.name] = user.id  

                    self.superSet = self.superSet.union(users)

            setOfUsers = list(self.superSet)
    
            print(user_id_name_map)
            user_ids = [user_id_name_map[user_name] for user_name in setOfUsers if user_name != 'MatchIDs']

            setOfUsers.append('MatchIDs')
            user_ids.append('UserIDs')

            with open(self.fname, 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(setOfUsers)
                writer.writerow(user_ids)
                
            dictOfFile = self.parse_poll_string(messageContent)
            print (f"File and list created: {dictOfFile}")
            self.logger.info(f"File and list created: {dictOfFile}")

            # Record Game
            i = 1
            for messageID in self.messageIDs:
                self.logger.info(f"Game {i}")
                msg = await message.channel.fetch_message(messageID)
                j=0
                
                # Get list of users from a message
                for reaction in msg.reactions:
                    reactionStr = str(reaction)
                    
                    # If j == 0, then it's the first team, if j == 1, then it's the second team
                    if j == 0:
                        if reactionStr in self.emojiListBack.keys():
                            teamAName = self.emojiListBack[reactionStr]
                        else:
                            teamAName = "A"
                    elif j == 1:
                        if reactionStr in self.emojiListBack.keys():
                            teamBName = self.emojiListBack[reactionStr]
                        else:
                            teamBName = "B"

                    # get the list of users for a team
                    users = set()
                    user_id_name_map = {} 
                    async for user in reaction.users():
                        users.add(user.name)
                        user_id_name_map[user.name] = user.id  

                    # Create team A list and create team B list
                    if j == 0:
                        teamAList = list(users)
                        teamCount = len(teamAList)
                    elif j == 1:
                        teamBList = list(users)
                        teamCount = len(teamBList)

                    teamCount = teamCount - 1
                    print (reactionStr + ' has count: ' + str(teamCount))
                    self.logger.info(f"{reactionStr} has count: {teamCount}")
                    j+=1

                # read the CSV file and get the user names from CSV
                df = pd.read_csv(self.fname)
                specific_matchID = "MATCH " + str(i)

                # Filter the DataFrame to select rows with the specific matchID
                match_rows = df[df["MatchIDs"] == specific_matchID]

                if match_rows.empty:
                    new_row = {"MatchIDs": specific_matchID}
                    df.loc[len(df)] = new_row
                
                # For each name in name of columns if column is not of MatchIDs          
                for column in df.columns:
                    if column != "MatchIDs":
                        if column in teamBList and column not in teamAList:
                            df[column] = df[column].astype(object)
                            df.loc[df['MatchIDs'] == specific_matchID, column] = teamBName
                        elif column in teamAList and column not in teamBList:
                            df[column] = df[column].astype(object)
                            df.loc[df['MatchIDs'] == specific_matchID, column] = teamAName
                        else:
                            df[column] = df[column].astype(object)
                            df.loc[df['MatchIDs'] == specific_matchID, column] = "N/A"
                
                # If a user doesn't exist in original list, they're added to the list with N/A and then their pick is updated for the match
                for user in teamAList:
                    if user not in df.columns:
                        df[user] = "N/A"
                        df[user] = df[user].astype(object)
                        df.loc[df['MatchIDs'] == "UserIDs", user] = user_id_name_map[user]
                        df.loc[df['MatchIDs'] == specific_matchID, user] = teamAName

                for user in teamBList:
                    if user not in df.columns:
                        df[user] = "N/A"
                        df[user] = df[user].astype(object)
                        df.loc[df['MatchIDs'] == "UserIDs", user] = user_id_name_map[user]
                        df.loc[df['MatchIDs'] == specific_matchID, user] = teamBName

                # Update the original DataFrame with the modified match rows
                df.to_csv(self.fname, index=False)

                print ("Game is now closed")
                self.logger.info("Game is now closed")
                i = i + 1

            self.messages = []
            self.superSet   = set()
            self.teamUsersA = []
            self.teamUsersB = []
            self.outputList = []

            await message.channel.send(f"{dictOfFile} recorded")                

        # Get the rank for a specific input <+rank {Type} {League}>
        if message.content.startswith("+rank"):
            messageContent = message.clean_content
            guildId  = message.guild.id
            userName = message.author.name
            userId   = message.author.id
            outputDict = self.parse_rank_string(messageContent)
            conn = sqlite3.connect(f"VCT_2024_{guildId}.db")

            if outputDict["Type"] == 'type':
                print(f"TYPE TYPE: {outputDict}")

                df = self.get_type_sum(conn, outputDict["type"], userName, userId)
                embed = self.tabulate_df(df, userName, outputDict["type"])
                await message.channel.send(embed=embed)

            elif outputDict["Type"] == 'league':
                print(f"LEAGUE TYPE: {outputDict}")

                df = self.get_league_sum(conn, outputDict["league"], userName, userId)
                embed = self.tabulate_df(df, userName, outputDict["league"])
                await message.channel.send(embed=embed)

            elif outputDict["Type"] == 'specific':
                print(f"SPECIFIC TYPE: {outputDict}")

                df = self.get_specific_sum(conn, outputDict["league"], outputDict["type"], userName, userId)
                embed = self.tabulate_df(df, userName, f'{outputDict["league"]} {outputDict["type"]}')
                await message.channel.send(embed=embed)

            elif outputDict["Type"] == 'latest':
                print(f"LATEST Type: {outputDict}")

                with open("etc/latest.txt", 'r') as file:
                    latest = file.read()
                if latest.lower() in ["kickoff", "il1", "il2", "champions"]:
                    df = self.get_type_sum(conn, latest.lower(), userName, userId)
                    embed = self.tabulate_df(df, userName, latest.lower())
                    await message.channel.send(embed=embed)
                elif latest.lower() in ["madrid", "shanghai","korea"]:
                    df = self.get_league_sum(conn, latest.lower(), userName, userId)
                    embed = self.tabulate_df(df, userName, latest.lower())
                    await message.channel.send(embed=embed)
                
            elif outputDict["Type"] == 'all':
                print(f"ALL TYPE: {outputDict}")

                df = self.get_all_sum(conn, userName, userId)
                embed = self.tabulate_df(df, userName, "2024")
                await message.channel.send(embed=embed)
            elif outputDict["Type"] == '404':
                print(f"FALSE TYPE: {outputDict}")


            conn.close()
            

    # Start the bot
    def run(self):
        super().run(self.config["discord_token"], reconnect=True)