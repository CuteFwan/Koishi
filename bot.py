from discord.ext import commands
import discord
import datetime
import logging
import json
import asyncio
import aiohttp
import asyncpg
import sys


# unnecessary stuff copy pasted in mostly
with open('config.json', 'r') as f:
    config = json.load(f)
BOT_INVITE = config["BOT_INVITE"]
SERVER_INVITE = config["SERVER_INVITE"]
DB_URI = config["DB_URI"]
TOKEN = config["TOKEN"]
STARTUP_EXTENSIONS = config["STARTUP_EXTENSIONS"]
ADMINS = config["ADMINS"]
AVY_GUILD = config["AVY_GUILD"]
AVY_CHANNEL = config["AVY_CHANNEL"]
DEFAULT_PREFIX = config["DEFAULT_PREFIX"]

logging.basicConfig(level=logging.INFO)

log_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')

discord_logger = logging.getLogger('discord')

discord_file_handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
discord_file_handler.setFormatter(log_formatter)
discord_logger.addHandler(discord_file_handler)

logger = logging.getLogger('koishi')

console_log_handler = logging.StreamHandler(sys.stdout)
console_log_handler.setFormatter(log_formatter)
logger.addHandler(console_log_handler)

koishi_file_handler = logging.FileHandler(filename='koishi.log', encoding='utf-8', mode='w')
koishi_file_handler.setFormatter(log_formatter)
logger.addHandler(koishi_file_handler)

coglogger = logging.getLogger('cogs')
coglogger.addHandler(koishi_file_handler)

description = '''Lies and slander follow'''
bot = commands.AutoShardedBot(command_prefix=commands.when_mentioned_or(DEFAULT_PREFIX), description=description)
bot.avy_guild = AVY_GUILD
bot.avy_channel = AVY_CHANNEL
bot.admins = ADMINS
bot.bot_invite = BOT_INVITE
bot.server_invite = SERVER_INVITE


@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    logger.info(f'id {bot.user.id}')
    logger.info(f'Running {discord.__version__}')


@bot.command(hidden=True)
@commands.is_owner()
async def backupreload(ctx, extension_name: str):
    bot.unload_extension(extension_name)
    try:
        bot.load_extension(extension_name)
    except (AttributeError, ImportError) as e:
        await ctx.send("```py\n{}: {}\n```".format(type(e).__name__, str(e)))
        return
    logger.info(f'{extension_name} reloaded.')
    await ctx.send("{} reloaded.".format(extension_name))


@bot.command(hidden=True)
@commands.is_owner()
async def logout(ctx):
    await ctx.send('goodbye')
    await bot.logout()


@bot.event
async def on_message(message):
    if message.author.bot:
        return
    await bot.process_commands(message)


async def create_pool(uri, **kwargs):
    """
        Experimenting with setting up pool with init.
    """
    def converter(data):
        if isinstance(data, datetime.datetime):
            return data.__str__()

    def _encode_jsonb(data):
        return json.dumps(data, default=converter)

    def _decode_jsonb(data):
        return json.loads(data)

    extra_init = kwargs.pop('init', None)

    async def init(conn):
        await conn.set_type_codec('jsonb', schema='pg_catalog', encoder=_encode_jsonb, decoder=_decode_jsonb, format='text')
        if extra_init is not None:
            await extra_init(conn)
    return await asyncpg.create_pool(uri, init=init, **kwargs)


async def run():
    try:
        pool = await create_pool(DB_URI)
        logger.info('Connected to postgresql server')
    except Exception as e:
        logger.exception('Could not set up postgresql')
        return
    bot.session = aiohttp.ClientSession()
    bot.pool = pool
    bot.start_time = datetime.datetime.utcnow()
    try:
        await bot.start(TOKEN)
    except KeyboardInterrupt:
        await bot.logout()
    finally:
        loop.close()
        
    
if __name__ == "__main__":
    for extension in STARTUP_EXTENSIONS:
        try:
            bot.load_extension(extension)
        except Exception as e:
            logger.exception(f'Failed to load extension {extension}.')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
