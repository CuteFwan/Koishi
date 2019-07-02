from discord.ext import commands
import time
import datetime
from .utils import pretty
import logging


class Basic(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger('koishi')
    
    @commands.command()
    async def ping(self, ctx):
        start = time.perf_counter()
        resp = await ctx.send('pong!')
        end = time.perf_counter()
        rtt = (end - start)*1000
        waaa = resp.created_at - ctx.message.created_at
        self.logger.info(f'{waaa} {waaa.total_seconds()} {waaa.microseconds}')
        ts = waaa.total_seconds() * 1000
        id = (resp.id - ctx.message.id) >> 22
        ws = self.bot.latency * 1000
        await resp.edit(content = f'{resp.content}\nrtt: {rtt:.2f}ms\nts: {ts:.2f}ms\nid: {id:.2f}ms\nws: {ws:.2f}ms')
    
    @commands.command()
    async def uptime(self, ctx):
        current_time = datetime.datetime.utcnow()
        time = pretty.delta_to_str(self.bot.start_time, current_time)
        
        await ctx.send(f'for {time} so far')

    
def setup(bot):
    bot.add_cog(Basic(bot))
