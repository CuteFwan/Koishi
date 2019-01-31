import discord
from discord.ext import commands
import datetime
import imp
import os
import time
import aiohttp
import asyncio


class Pop:
    def __init__(self, bot):
        self.bot = bot
        self.pending_status_updates = {}
        self.bg_task = self.bot.loop.create_task(self.batching_task())
        self.synced = asyncio.Event()
        
    def __unload(self):
        print('die')
        utcnow = datetime.datetime.utcnow()
        self.bg_task.cancel()
        self.pending_status_updates.append((0, 'cog_offline', utcnow))


    async def __local_check(self, ctx):
        if ctx.author.id in [109778500260528128, 145802776247533569]:
            return True
        return False

    async def batching_task(self): #modify this to work with others
        try:
            await self.bot.wait_until_ready()
            await self.synced.wait()
            while True:
                await asyncio.sleep(5)
                await self.insert_to_db()
                print("INSERTING")
        except asyncio.CancelledError:
            print('batching task was cancelled')
            await self.insert_to_db()
            if self.pending_status_updates:
                print(f'{len(self.pending_status_updates)} status updates DIED')
        print('exited?')

    @commands.command()
    async def stahp(self, ctx):
        self.__unload()

    async def insert_to_db(self):
        to_insert = self.pending_status_updates
        self.pending_status_updates = []
        print(f'trying to insert {len(to_insert)}')
        async with self.bot.pool.acquire() as con:
            result = await con.copy_records_to_table('statuses', records=to_insert, columns=['uid','status','first_seen'],schema_name='koi_test')
        
    async def on_ready(self):
        utcnow = datetime.datetime.utcnow()

        await self.bot.request_offline_members(*[guild for guild in self.bot.guilds if guild.large])
        
        stuff_to_do = [(m.id, m.status.name, utcnow) for m in list(set(self.bot.get_all_members()))]
        self.pending_status_updates.extend(stuff_to_do)
        await self.insert_to_db()
        self.synced.set()
        print("synced!")


    async def on_member_update(self, before, after):
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()

        lowest = discord.utils.find(lambda x: x.get_member(after.id) is not None, sorted(self.bot.guilds, key=lambda x: x.id)) # stolen from luma I think
        
        if after.guild.id == lowest.id:
            if before.status != after.status:
                
                self.pending_status_updates.append((after.id, after.status.name, utcnow))
                

    
def setup(bot):
    bot.add_cog(Pop(bot))