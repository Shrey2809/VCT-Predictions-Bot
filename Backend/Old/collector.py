import discord
from pollBot import PollBot
from collectBot import CollectReactions
import sys
import json

with open('configVCT.json') as config_file:
    config = json.load(config_file)

bot = CollectReactions(config)
bot.run()