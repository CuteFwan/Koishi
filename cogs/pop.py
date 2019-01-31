import discord
from discord.ext import commands
import datetime
import imp
import os
import time
import aiohttp
import asyncio


scheme = {
         'names' : ['uid','name','first_seen'],
         'avatars' : ['uid','avatar','avatar_url','path','first_seen'],
         'discrims' : ['uid','discrim','first_seen'],
         'nicks' : ['uid','sid','nick','first_seen'],
         'statuses' : ['uid','status','first_seen'],
         'games' : ['uid','game','first_seen']
         }


class Pop:
    def __init__(self, bot):
        self.bot = bot
        self.pending_updates = {recordtype : [] for recordtype in scheme.keys()}
        self.bg_tasks = {recordtype : self.bot.loop.create_task(self.batching_task(recordtype)) for recordtype in scheme.keys()}
        #self.bg_tasks = [{'statuses' : self.bot.loop.create_task(self.batching_task('statuses'))}]
        self.synced = asyncio.Event()
        
    def __unload(self):
        print('die')
        utcnow = datetime.datetime.utcnow()
        self.bg_task['names'].cancel()
        self.pending_updates['names'].append((0, 'cog_offline', utcnow))
        self.bg_task['avatars'].cancel()
        self.pending_updates['avatars'].append((0, 'cog_offline', 'cog_offline', 'cog_offline', utcnow))
        self.bg_task['discrims'].cancel()
        self.pending_updates['discrims'].append((0, 'cog_offline', utcnow))
        self.bg_task['nicks'].cancel()
        self.pending_updates['nicks'].append((0, 0, 'cog_offline', utcnow))
        self.bg_task['statuses'].cancel()
        self.pending_updates['statuses'].append((0, 'cog_offline', utcnow))
        self.bg_task['games'].cancel()
        self.pending_updates['games'].append((0, 'cog_offline', utcnow))


    async def batching_task(self, recordtype, interval : int = 5): #modify this to work with others
        print(f'started {recordtype} task')
        try:
            interval = min(max(1,interval),60)
            await self.bot.wait_until_ready()
            await self.synced.wait()
            while True:
                await asyncio.sleep(interval)
                await self.insert_to_db(recordtype)
        except asyncio.CancelledError:
            print(f'Batching task for {recordtype} was cancelled')
            await self.insert_to_db(recordtype)
            if self.pending_updates[recordtype]:
                print(f'{len(self.pending_updates[recordtype])} status updates DIED')
        print(f'exited {recordtype} task')

    async def insert_to_db(self, recordtype):
        to_insert = self.pending_updates[recordtype]
        if len(to_insert) == 0:
            return
        self.pending_updates[recordtype] = []
        print(f'trying to insert {len(to_insert)} to {recordtype}')
        async with self.bot.pool.acquire() as con:
            result = await con.copy_records_to_table(recordtype, records=to_insert, columns=scheme[recordtype],schema_name='koi_test')

    async def on_ready(self):
        utcnow = datetime.datetime.utcnow()

        await self.bot.request_offline_members(*[guild for guild in self.bot.guilds if guild.large])
        
        for m in list(set(self.bot.get_all_members())):
            self.pending_updates['names'].append((m.id, m.name, utcnow))
            self.pending_updates['avatars'].append((
                                                    m.id,
                                                    m.avatar if m.avatar else m.default_avatar.name,
                                                    m.avatar_url_as(static_format='png'),
                                                    f'/{m.id}/{m.avatar}.{"gif" if m.is_avatar_animated() else "png"}',
                                                    utcnow
                                                  ))
            self.pending_updates['discrims'].append((m.id, m.discriminator, utcnow))
            self.pending_updates['nicks'].append((m.id, m.guild.id, m.nick, utcnow))
            self.pending_updates['statuses'].append((m.id, m.status.name, utcnow))
            self.pending_updates['games'].append((m.id, m.activity.name if m.activity else None, utcnow))
        await asyncio.gather(*[self.insert_to_db(recordtype) for recordtype in scheme.keys()])
        self.synced.set()
        print("synced!")


    async def on_member_update(self, before, after):
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        aid = after.id

        if before.name != after.name:
            self.pending_updates['names'].append((aid, after.name, utcnow))
        if before.avatar != after.avatar:
            self.pending_updates['avatars'].append((
                                                    aid,
                                                    after.avatar if after.avatar else after.default_avatar.name,
                                                    after.avatar_url_as(static_format='png'),
                                                    f'/{aid}/{after.avatar}.{"gif" if after.is_avatar_animated() else "png"}',
                                                    utcnow
                                                  ))
        if before.discriminator != after.discriminator:
            self.pending_updates['discrims'].append((aid, after.discriminator, utcnow))
        if before.nick != after.nick:
            self.pending_updates['nicks'].append((aid, after.guild.id, after.nick, utcnow))

        lowest = discord.utils.find(lambda x: x.get_member(aid) is not None, sorted(self.bot.guilds, key=lambda x: x.id)) # stolen from luma I think
        
        if after.guild.id == lowest.id:
            if before.status != after.status:
                self.pending_updates['statuses'].append((aid, after.status.name, utcnow))
            if before.activity != after.activity and not after.bot:
                self.pending_updates['games'].append((aid, after.activity.name if after.activity else None, utcnow))

    @commands.command()
    async def stahp(self, ctx):
        self.__unload()
                

    
def setup(bot):
    bot.add_cog(Pop(bot))