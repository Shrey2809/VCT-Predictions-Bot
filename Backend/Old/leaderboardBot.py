import discord
from discord.ext import commands
import logging
import aiohttp
import csv
import random
import json
import pandas as pd, numpy as np
from tabulate import tabulate

class LeaderboardBot(commands.AutoShardedBot):
    def __init__(self, config):
        prefixes = ["+", "poll:", "Poll:", "POLL:"]

        super().__init__(status = discord.Status.online, command_prefix = "+score")
        self.config = config
        self.shard_count = self.config["shards"]["count"]
        shard_ids_list = []
        shard_ids = []
        
        # create list of shard ids
        for i in range(self.config["shards"]["first_shard_id"], self.config["shards"]["last_shard_id"]+1):
            shard_ids_list.append(i)
        self.shard_ids = tuple(shard_ids_list)


        self.MASTERS_TOKYO_PATH  = "Records\CHAMPS2023.xlsm"
        self.raw_dataframe = pd.read_excel(self.MASTERS_TOKYO_PATH).sort_values(by=["Total", "Playoffs Points"], ascending=False, ignore_index=True)
        self.raw_dataframe = self.raw_dataframe[self.raw_dataframe["Name"] != "Max Points"]
        self.raw_dataframe["Rank"] = range(1, len(self.raw_dataframe) + 1)
        self.usernameToCheck = ''

    
    def get_df_based_on_name(self, name):
        raw_dataframe = pd.read_excel(self.MASTERS_TOKYO_PATH).sort_values("Total", ascending=False)
        raw_dataframe = raw_dataframe[raw_dataframe["Name"] != "Max Points"]
        raw_dataframe["Rank"] = range(1, len(raw_dataframe) + 1)
        filtered_df = raw_dataframe[raw_dataframe['Name'] == name]
        filtered_df = filtered_df[["Rank", "Name", "Playoffs Points", "Total"]]
        return filtered_df


    async def on_ready(self):
        self.http_session = aiohttp.ClientSession()
        print("Leaderboard Bot Online")
        print("---------------")


    async def on_message(self, message):
        #self.usernameToCheck = self.remove_after_character(message.author, '#')
        self.usernameToCheck = message.author.name
        if self.usernameToCheck == "axon319":
            if message.content.startswith("+top"):
                self.raw_dataframe = pd.read_excel(self.MASTERS_TOKYO_PATH).sort_values(by=["Total", "Playoffs Points"], ascending=False, ignore_index=True)
                self.raw_dataframe = self.raw_dataframe[self.raw_dataframe["Name"] != "Max Points"]
                self.raw_dataframe["Rank"] = range(1, len(self.raw_dataframe) + 1)

                tempdf = self.raw_dataframe.head(10)[["Rank", "Name", "Playoffs Points", "Total"]]
                table = tabulate(tempdf, headers='keys', tablefmt="heavy_outline", showindex="never")

                embed = discord.Embed(title='Leaderboard: Top 10', color=0xB083ff)
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)

            if message.content.startswith("+get_top"):
                headVal = int(message.clean_content[len("+get_top "):])
                self.raw_dataframe = pd.read_excel(self.MASTERS_TOKYO_PATH).sort_values(by=["Total", "Playoffs Points"], ascending=False, ignore_index=True)
                self.raw_dataframe = self.raw_dataframe[self.raw_dataframe["Name"] != "Max Points"]
                self.raw_dataframe["Rank"] = range(1, len(self.raw_dataframe) + 1)

                tempdf = self.raw_dataframe.head(headVal)[["Rank", "Name", "Playoffs Points", "Total"]]
                table = tabulate(tempdf, headers='keys', tablefmt="heavy_outline", showindex="never")

                embed = discord.Embed(title=f'Leaderboard: Top {headVal}', color=0xB083ff)
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)
           
        if message.content.startswith("+end") or message.content.startswith("+exit"):
            await message.channel.send("Bot's going offline!")
            exit()
        
        if message.content.startswith("+rank"):
            print(f"USername: {self.usernameToCheck}")
            tempdf = self.get_df_based_on_name(self.usernameToCheck)[["Rank", "Playoffs Points", "Total"]]
            table = tabulate(tempdf, headers='keys', tablefmt="simple_outline", showindex="never")

            embed = discord.Embed(title=f"{self.usernameToCheck}'s Rank", color=0xB083ff)
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)


    def run(self):
        super().run(self.config["discord_token"], reconnect=True)