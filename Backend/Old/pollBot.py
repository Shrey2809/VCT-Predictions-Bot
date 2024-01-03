import discord
from discord.ext import commands
import logging
import aiohttp
import csv
import random
import json
import pandas as pd

class PollBot(commands.AutoShardedBot):
    def __init__(self, config):
        prefixes = ["+", "poll:", "Poll:", "POLL:"]
        self.emojiLetters = [
            "\N{REGIONAL INDICATOR SYMBOL LETTER A}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER B}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER C}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER D}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER E}", 
            "\N{REGIONAL INDICATOR SYMBOL LETTER F}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER G}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER H}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER I}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER J}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER K}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER L}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER M}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER N}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER O}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER P}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER Q}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER R}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER S}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER T}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER U}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER V}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER W}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER X}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER Y}",
            "\N{REGIONAL INDICATOR SYMBOL LETTER Z}"
        ]


        # Opening JSON file
        with open('VCTPartnershipEmotes.json') as json_file:
            self.emojiList = json.load(json_file)

        self.team1 = []
        self.team2 = []
        super().__init__(status = discord.Status.online, command_prefix = "+poll")
        self.config = config
        self.shard_count = self.config["shards"]["count"]
        shard_ids_list = []
        shard_ids = []
        
        # create list of shard ids
        for i in range(self.config["shards"]["first_shard_id"], self.config["shards"]["last_shard_id"]+1):
            shard_ids_list.append(i)
        self.shard_ids = tuple(shard_ids_list)

        self.remove_command("help")
        self.messages = []

    
    def get_df_based_on_name(self, raw_dataframe, name):
        filtered_df = raw_dataframe[raw_dataframe['Name'] == name]
        filtered_df = filtered_df[["Rank", "Name", "Total"]]
        return filtered_df

    def remove_after_character(self, string, character):
        parts = string.split(character, 1)  # Split the string into two parts at the first occurrence of the character
        if len(parts) > 1:
            return parts[0]  # Return the first part before the character
        else:
            return string  # Return the original string if the character is not found

    # parses the title, which should be in between curly brackets ('{ title }')
    def find_title(self, message):
        # this is the index of the first character of the title
        first = message.find('{') + 1
        # index of the last character of the title
        last = message.find('}')
        if first == 0 or last == -1:
            return "Not using the command correctly"
        return message[first:last]

    def output_poll(self, title):
        fileName = "Games\\" + title + ".csv"
        file     = open(fileName, "r")
        i = 1
        for row in file:
            r = row.split(',')
            self.team1.append(r[0])
            self.team2.append(r[1].strip())
        print(self.team1)
        print(self.team2)
        file.close()

    def get_team_emote(self, team, i):
        try:
            emote = self.emojiList[team]
        except:
            emote = self.emojiLetters[i]
        return emote

    

    async def on_ready(self):
        self.http_session = aiohttp.ClientSession()
        print("Poll Bot Online")
        print("---------------")


    async def on_message(self, message):
        # if self.usernameToCheck == "axon319":
            if message.content.startswith("+poll"):
                messageContent = message.clean_content
                if messageContent.find("{") == -1:
                    await message.add_reaction(self.emojiLetters[0])
                    await message.add_reaction(self.emojiLetters[1])
                else:
                    title = self.find_title(messageContent)

                    if title[0:7] == "PACIFIC" or title[0:7] == "pacific" :
                        tLen      = len("PACIFIC")
                        pollColor = 0x01D2D7
                        league    = self.emojiList["PACIFIC"]
                    elif title[0:8] == "AMERICAS" or title[0:8] == "americas" :
                        tLen      = len("AMERICAS")
                        pollColor = 0xFF570C
                        league    = self.emojiList["AMERICAS"]
                    elif title[0:4] == "EMEA" or title[0:4] == "emea" :
                        tLen      = len("EMEA")
                        pollColor = 0xDC3030
                        league    = self.emojiList["EMEA"]
                    elif title[0:len("MST")] == "MST":
                        print ("MST")
                        tLen      = len("MST")
                        pollColor = 0xB083ff
                        league    = self.emojiList["MST"]
                    elif title[0:len("CHAMPS")] == "CHAMPS":
                        tLen      = len("CHAMPS")
                        pollColor = 0xD22FE7
                        league    = self.emojiList["CHAMPS"]

                    week = title[tLen:]

                    if title[0:2] == "PA" or title[0:2] == "AM" or title[0:2] == "EM" or title[0:2] == "MS" or title[0:2] == "CH":
                        self.output_poll(title)
                        self.maxGames = len(self.team1)
                        j = 1
                        while j <= self.maxGames:
                            pointsWorth = input("Enter points for this game: ")
                            if pointsWorth == '1':
                                print("point is 1")
                                pointStr = f"{pointsWorth} point"
                            else:
                                print("point is not 1")
                                pointStr = f"{pointsWorth} points"

                            pollMessage = ""
                            options = []
                            i = 0;
                            options.append(self.team1[j-1])
                            options.append(self.team2[j-1])
                            teamA = self.team1[j-1]
                            teamB = self.team2[j-1]

                            print ("TEAM A: "+teamA)
                            print ("TEAM B: "+teamB)

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

                            # pollMessage = pollMessage + f"\n\t\t\t{pointsWorth} point(s)"

                            boxTitle = f"**{league} S{week} G{j}: {self.team1[j-1]} vs {self.team2[j-1]} ({pointStr})**"
                            e = discord.Embed(title=boxTitle, description=pollMessage, colour=pollColor)
                            pollMessage = await message.channel.send(embed=e)
                            self.messages.append(pollMessage)

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

                        msgIds = []
                        with open(f'IDs\\{title}.txt', 'w') as id_file:
                            for msg in self.messages:
                                msgIds.append(msg.id)
                                id_file.write(f"{msg.id}\n")

                        self.messages = []
                        self.team1 = []
                        self.team2 = []

                        
            if message.content.startswith("+end") or message.content.startswith("+exit"):
                await message.channel.send("Bot's going offline!")
                exit()
            

    def run(self):
        super().run(self.config["discord_token"], reconnect=True)