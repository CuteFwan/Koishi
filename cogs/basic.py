import discord
from discord.ext import commands
import time
import datetime

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
    
    @commands.command()
    async def uptime(self, ctx):
        current_time = datetime.datetime.utcnow()
        diff = current_time - self.bot.start_time
        d, s = divmod(diff.seconds + diff.days * 86400, 86400)
        h, s = divmod(s, 3600)
        m, s = divmod(s, 60)
        msg = ''
        if d != 0:
            msg += '{}d {}h {}m'.format(d, h, m)
        elif h != 0:
            msg += '{}h {}m'.format(h, m)
        else:
            msg += '{}m'.format(m)
        print(f'{datetime.datetime.now()} - {msg} {s}s')
        await ctx.send(f'for {msg} so far')

    
def setup(bot):
    bot.add_cog(Basic(bot))