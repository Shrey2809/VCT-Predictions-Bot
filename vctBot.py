import discord
from vctBotBackend import vctBotBackend
import sys
import json

with open('etc/config.json') as config_file:
    config = json.load(config_file)

bot = vctBotBackend(config)
bot.run()