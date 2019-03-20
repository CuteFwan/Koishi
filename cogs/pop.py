import discord
from discord.ext import commands
import datetime
import imp
import os
import time
import aiohttp
import asyncio
from collections import deque
from .utils import images
from io import BytesIO
from PIL import Image

scheme = {
         'names' : {
            'uid' : 'BIGINT',
            'name' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'avatars' : {
            'uid' : 'BIGINT',
            'avatar' : 'TEXT',
            'avatar_url' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'discrims' : {
            'uid' : 'BIGINT',
            'discrim' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'nicks' : {
            'uid' : 'BIGINT',
            'sid' : 'BIGINT',
            'nick' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'statuses' : {
            'uid' : 'BIGINT',
            'status' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'games' : {
            'uid' : 'BIGINT',
            'game' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            }
         }


class Pop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.pending_updates = {recordtype : [] for recordtype in scheme.keys()}
        self.avatars = []
        #self.bg_tasks = {recordtype : self.bot.loop.create_task(self.batching_task(recordtype)) for recordtype in scheme.keys()}
        #self.bg_avy_task = self.bot.loop.create_task(self.batch_post_avatars())

        self.bg_tasks = {'avatars' : self.bot.loop.create_task(self.batching_task('avatars'))}
        self.synced = asyncio.Event()
        self.wh = None
        self.first_synced = False
        
    def cog_unload(self):
        print('die')
        utcnow = datetime.datetime.utcnow()
        for recordtype in scheme.keys():
            print(f'canceling {recordtype}')
            self.bg_tasks[recordtype].cancel()
        self.fill_updates(0, 0, 'cog_offline', utcnow, True)


    async def batching_task(self, recordtype, interval : int = 5):
        print(f'started {recordtype} task')
        try:
            interval = min(max(1,interval),60)
            await self.bot.wait_until_ready()
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
            result = await con.copy_records_to_table(recordtype, records=to_insert, columns=scheme[recordtype].keys(),schema_name='koi_test')
            print(result)

    async def insert_to_db_2(self, recordtype):
        to_insert = self.pending_updates[recordtype]
        if len(to_insert) == 0:
            return
        self.pending_updates[recordtype] = []
        print(f'trying to insert {len(to_insert)} to {recordtype}')
        names = scheme[recordtype].keys()
        cols = ', '.join(names)
        types = ', '.join(f'{k} {v}' for k, v in scheme[recordtype].items())
        transformed = [{col : row[i] for i, col in enumerate(names)} for row in to_insert]
        query = f'''
                insert into {recordtype} ({cols})
                select {cols}
                from jsonb_to_recordset($1::jsonb) as x({types})
                '''
        await self.bot.pool.execute(query, transformed)

    async def batch_post_avatars(self):
        print('started avatar posting task')
        try:
            print('try')
            await self.bot.wait_until_ready()
            while True:
                print('while')
                while not self.wh:
                    self.wh = discord.utils.get(await self.bot.get_guild(self.bot.avy_guild).webhooks(), channel_id=self.bot.avy_channel)
                    if self.wh:
                        print(f'found webhook {self.wh.name} for {self.bot.avy_channel}')
                        break
                    else:
                        print('waiting for wh')
                        await asyncio.sleep(2)
                while len(self.avatars) == 0:
                    print('waiting for avatars')
                    await asyncio.sleep(2)
                print(f'{len(self.avatars)} avatars added')
                posting_queue = self.avatars.copy()
                self.avatars.clear()
                hashes_to_check = set([e[0] for e in posting_queue])
                
                query = '''
                    select hash
                    from avy_urls
                    where
                        hash = any($1::text[])
                '''
                results = await self.bot.pool.fetch(query, hashes_to_check)
                results = [r['hash'] for r in results]
                posting_queue = [avy for avy in posting_queue if avy[0] not in results]

                print(f'{len(posting_queue)} avatars to dl and post')

                async def url_to_bytes(hash, url, session):
                    async with session.get(url) as r:
                        return (hash, BytesIO(await r.read()))

                posting_queue = deque(await asyncio.gather(*[url_to_bytes(avy[0], avy[1], self.bot.session) for avy in posting_queue]))

                while len(posting_queue) > 0:
                    to_post = {}
                    post_size = 0
                    while len(to_post) < 10 and len(posting_queue) > 0:
                        avy = posting_queue.popleft()
                        s = avy[1].getbuffer().nbytes
                        if post_size + s < 8000000:
                            post_size += s
                            to_post[avy[0]] = discord.File(avy[1], filename=f'{avy[0]}.{"png" if not avy[0].startswith("a_") else "gif"}')
                        elif s > 8000000:
                            new_bytes = None
                            if avy[0].startswith('a_'):
                                new_bytes = await self.bot.loop.run_in_executor(None, self._extract_first_frame, avy[1])
                                
                            else:
                                new_bytes = await self.bot.loop.run_in_executor(None, images.resize_to_limit, avy[1], 8000000)
                            posting_queue.appendleft((avy[0], new_bytes))
                        else:
                            posting_queue.appendleft((avy[0], avy[1]))
                    if len(to_post) == 0:
                        #Nothing left to post
                        break
                    try:
                        message = await self.wh.send(content='\n'.join(to_post.keys()), wait=True, files=to_post.values())
                        transformed = [
                            {
                                'hash' : a.filename[::-1].split('.', 1)[-1][::-1],
                                'url' : a.url,
                                'msgid' : message.id,
                                'id' : a.id,
                                'size' : a.size,
                                'height' : a.height,
                                'width' : a.width
                            } for a in message.attachments
                        ]
                        query = '''
                            insert into avy_urls
                            (hash, url, msgid, id, size, height, width)
                            select x.hash, x.url, x.msgid, x.id, x.size, x.height, x.width
                            from jsonb_to_recordset($1::jsonb) as x(hash text, url text, msgid bigint, id bigint, size bigint, height bigint, width bigint)
                            on conflict (hash) do nothing
                        '''
                        await self.bot.pool.execute(query, transformed)
                    except discord.HTTPException:
                        print('something happened')
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            print('Batching task for avatar posting was cancelled')

    def _extract_first_frame(self, data):
        with Image.open(data) as im:
            im = im.convert('RGBA')
            b = BytesIO()
            im.save(b, 'gif')
            b.seek(0)
            return b

    @commands.Cog.listener()
    async def on_ready(self):
        if self.first_synced == False:
            utcnow = datetime.datetime.utcnow()

            await self.bot.request_offline_members(*[guild for guild in self.bot.guilds if guild.large])
            self.fill_updates(0, 0, 'cog_online', utcnow, True)
            self.add_bulk_members(list(self.bot.get_all_members()), utcnow)
            self.synced.set()
            self.first_synced = True
            print("synced!")

    def add_bulk_members(self, members, utcnow):
        for m in members:
            self.pending_updates['nicks'].append((m.id, m.guild.id, m.nick, utcnow))
        print(len(list(set(members))))
        for m in list(set(members)):
            self.pending_updates['names'].append((m.id, m.name, utcnow))
            self.pending_updates['avatars'].append((
                                                    m.id,
                                                    m.avatar if m.avatar else m.default_avatar.name,
                                                    m.avatar_url_as(static_format='png'),
                                                    utcnow
                                                  ))
            self.pending_updates['discrims'].append((m.id, m.discriminator, utcnow))
            self.pending_updates['statuses'].append((m.id, m.status.name, utcnow))
            self.pending_updates['games'].append((m.id, m.activity.name if m.activity else None, utcnow))
            self.avatars.append((m.avatar if m.avatar else m.default_avatar.name, m.avatar_url_as(static_format='png')))


    def add_member(self, m, utcnow, full = True):
        self.pending_updates['nicks'].append((m.id, m.guild.id, m.nick, utcnow))
        if full:
            self.pending_updates['names'].append((m.id, m.name, utcnow))
            self.pending_updates['avatars'].append((
                                                    m.id,
                                                    m.avatar if m.avatar else m.default_avatar.name,
                                                    m.avatar_url_as(static_format='png'),
                                                    utcnow
                                                  ))
            self.pending_updates['discrims'].append((m.id, m.discriminator, utcnow))
            self.pending_updates['statuses'].append((m.id, m.status.name, utcnow))
            self.pending_updates['games'].append((m.id, m.activity.name if m.activity else None, utcnow))
            self.avatars.append((m.avatar if m.avatar else m.default_avatar.name, m.avatar_url_as(static_format='png')))

    def fill_updates(self, uid, sid, msg, utcnow, full = True):
        print(f'running fill_updates with {full}')
        self.pending_updates['nicks'].append((uid, sid, msg, utcnow))
        if full:
            self.pending_updates['names'].append((uid, msg, utcnow))
            self.pending_updates['avatars'].append((uid, msg, msg, msg, utcnow))
            self.pending_updates['discrims'].append((uid, msg, utcnow))
            self.pending_updates['statuses'].append((uid, msg, utcnow))
            self.pending_updates['games'].append((uid, msg, utcnow))

    @commands.Cog.listener()
    async def on_member_join(self, member):
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        do_full = sum(1 for g in self.bot.guilds if g.get_member(member.id)) == 1
        self.add_member(member, utcnow, do_full)

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        do_full = sum(1 for g in self.bot.guilds if g.get_member(member.id)) == 0
        self.fill_updates(member.id, member.guild.id, 'left_guild', utcnow, do_full) #untested stuff
    
    @commands.Cog.listener()
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
                                                    utcnow
                                                  ))
            self.avatars.append((after.avatar if after.avatar else after.default_avatar.name, after.avatar_url_as(static_format='png')))
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

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """
            It is rare to have too many dups of members in new guilds.
            Regardless, dups don't matter and are easy to deal with.
        """
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        self.add_bulk_members(guild.members, utcnow)
        print(f'Added {guild.member_count} people to queues!')

    @commands.Cog.listener()
    async def on_guild_remove(self, guild):
        """
            Figuring out which users the bot can still see is important.
            Need to find a better way to figure out if the user is in any other mutual guilds.
        """
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        for member in guild.members:
            if sum(1 for g in self.bot.guilds if g.get_member(member.id)) == 0:
                self.fill_updates(member.id, member.guild.id, 'left_guild', utcnow, True)
            else:
                self.fill_updates(member.id, member.guild.id, 'left_guild', utcnow, False)


                

    
def setup(bot):
    bot.add_cog(Pop(bot))