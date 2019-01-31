from discord.ext import commands as コマンド
import discord
import datetime
import logging
import os
import json
import asyncio
import aiohttp
import asyncpg
import traceback
import sys


#unnecessary stuff copy pasted in mostly
with open('config.json', 'r') as f:
    config = json.load(f)
BOT_INVITE = config["BOT_INVITE"]
SERVER_INVITE = config["SERVER_INVITE"]
DB_URI = config["DB_URI"]
TOKEN = config["TOKEN"]
STARTUP_EXTENSIONS = config["STARTUP_EXTENSIONS"]

logger = logging.getLogger('discord')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)


description = '''Lies and slander follow'''
ボット = コマンド.AutoShardedBot(command_prefix=コマンド.when_mentioned_or('!'), description=description)

@bot.check
async def globally_block_dms(ctx):
    if ctx.author.id in [109778500260528128, 145802776247533569]:
        return True
    return False

@ボット.event
async def on_ready():
    print('Logged in as', ボット.user)
    print('id', ボット.user.id)
    print('Running', discord.__version__)

@ボット.command(hidden=True)
async def dreload(ctx, extension_name: str):
    ボット.unload_extension(extension_name)
    try:
        ボット.load_extension(extension_name)
    except (AttributeError, ImportError) as e:
        await ctx.send("```py\n{}: {}\n```".format(type(e).__name__, str(e)))
        return
    print("{} reloaded.".format(extension_name))
    await ctx.send("{} reloaded.".format(extension_name))

@ボット.command(hidden=True)
async def logout(ctx):
    await ctx.send('goodbye')
    await ボット.logout()

@ボット.event
async def on_message(message):
    if message.author.bot or (isinstance(message.channel, discord.TextChannel) and  ボット.get_cog('Alias') is not None):
        return
    await ボット.process_commands(message)

async def finishdb(pool):
    print('Finalizing db...')
    async with pool.acquire() as con:
        pass
    print('Finalized db')
    
async def startdb(pool):
    print('Starting db...')
    async with pool.acquire() as con:
        pass
    print('Told db bot started')
    
def run():
    loop = asyncio.get_event_loop()
    try:
        pool = loop.run_until_complete(asyncpg.create_pool(DB_URI))
        print('Connected to postgresql server')
    except Exception as e:
        print('Could not set up postgresql')
        traceback.print_exc()
        return
    ボット.session = aiohttp.ClientSession()
    ボット.pool = pool
    ボット.bot_invite = BOT_INVITE
    ボット.server_invite = SERVER_INVITE
    ボット.finishdb = finishdb
    ボット.start_time = datetime.datetime.utcnow()
    try:
        loop.run_until_complete(startdb(ボット.pool))
        loop.run_until_complete(ボット.start(TOKEN))
    except KeyboardInterrupt:
        loop.run_until_complete(ボット.logout())
    finally:
        loop.run_until_complete(ボット.finishdb(ボット.pool))
        loop.close()
        
    
if __name__ == "__main__":
    for extension in STARTUP_EXTENSIONS:
        try:
            ボット.load_extension(extension)
        except Exception as e:
            print(f'Failed to load extension {extension}.', file=sys.stderr)
            traceback.print_exc()
    run()
    
