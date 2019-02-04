import discord
from discord.ext import commands
import time

class Basic:
    def __init__(self, bot):
        self.bot = bot

    
    @commands.command()
    async def ping(self, ctx):
        start = time.perf_counter()
        resp = await ctx.send('pong!')
        end = time.perf_counter()
        rtt = (end - start)*1000
        waaa = resp.created_at - ctx.message.created_at
        print(waaa, waaa.total_seconds(), waaa.microseconds)
        ts = waaa.total_seconds() * 1000
        id = (resp.id - ctx.message.id) >> 22
        ws = self.bot.latency * 1000
        await resp.edit(content = f'{resp.content}\nrtt: {rtt:.2f}ms\nts: {ts:.2f}ms\nid: {id:.2f}ms\nws: {ws:.2f}ms')
      
    
def setup(bot):
    bot.add_cog(Basic(bot))