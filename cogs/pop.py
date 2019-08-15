import discord
from discord.ext import commands
import datetime
import os
import aiohttp
import asyncio
from .utils import images
from io import BytesIO
import logging
from yarl import URL

scheme = {
         'names' : {
            'uid' : 'BIGINT',
            'name' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'avatars' : {
            'uid' : 'BIGINT',
            'avatar' : 'TEXT',
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
scheme2 = {
         'names' : {
            'key' : 'uid',
            'value' : 'name',
            },
         'avatars' : {
            'key' : 'uid',
            'value' : 'avatar',
            },
         'discrims' : {
            'key' : 'uid',
            'value' : 'discrim',
            },
         'nicks' : {
            'key' : 'uid, sid',
            'value' : 'nick',
            },
         'statuses' : {
            'key' : 'uid',
            'value' : 'status',
            },
         'games' : {
            'key' : 'uid',
            'value' : 'game',
            }
         }


class Pop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bg_tasks = {recordtype : self.bot.loop.create_task(self.batching_task(recordtype)) for recordtype in scheme.keys()}
        self.post_avy_task = self.bot.loop.create_task(self.batch_post_avatars())
        self.dl_avys_task = self.bot.loop.create_task(self.dl_avys())
        self.batch_remove_task = self.bot.loop.create_task(self.batch_member_remove())
        self.synced = asyncio.Event()
        self.wh = None
        
        self.bot.loop.create_task(self.first_sync())

        self.logger = logging.getLogger('koishi')


    def cog_unload(self):
        self.logger.info('die')
        utcnow = datetime.datetime.utcnow()
        self.post_avy_task.cancel()
        self.dl_avys_task.cancel()
        self.batch_remove_task.cancel()
        for recordtype, task in self.bg_tasks.items():
            self.logger.info(f'canceling {recordtype}')
            task.cancel()
        self.bot.loop.create_task(self.cog_log(False, utcnow))

    async def cog_log(self, start, time):
        event = 'cog_online' if start else 'cog_offline'
        query = '''insert into cog_log (event, time) values ($1, $2)'''
        await self.bot.pool.execute(query, event, time)

    async def batching_task(self, recordtype, interval : int = 5):
        self.logger.info(f'started {recordtype} task')
        try:
            interval = min(max(1,interval),60)
            await self.bot.wait_until_ready()
            while True:
                await asyncio.sleep(interval)
                await self.insert_to_db(recordtype)
        except asyncio.CancelledError:
            self.logger.warning(f'Batching task for {recordtype} was cancelled')
            await self.insert_to_db(recordtype)
            if self.bot.pending_updates[recordtype]:
                self.logger.error(f'{len(self.bot.pending_updates[recordtype])} status updates DIED')
        self.logger.info(f'exited {recordtype} task')

    async def insert_to_db(self, recordtype):
        to_insert = self.bot.pending_updates[recordtype][:]
        if len(to_insert) == 0:
            return
        self.bot.pending_updates[recordtype] = []
        async with self.bot.pool.acquire() as con:
            result = await con.copy_records_to_table(recordtype, records=to_insert, columns=scheme[recordtype].keys(),schema_name='koi_test')
            if len(to_insert) > 20000 and recordtype not in ['statuses','games']:
                key = scheme2[recordtype]['key']
                value = scheme2[recordtype]['value']
                query = f'''
                    delete from
                        {recordtype}
                    where
                        ref in (
                            select
                                ref
                            from (
                                select
                                    ref,
                                    {value},
                                    lead({value}) over (partition by {key} order by first_seen desc) as r_last,
                                    first_seen
                                from {recordtype}
                                order by first_seen desc
                            ) subtable
                            where
                                {value} = r_last
                        )
                '''
                await con.execute(query)

    async def insert_to_db_2(self, recordtype):
        to_insert = self.bot.pending_updates[recordtype][:]
        if len(to_insert) == 0:
            return
        self.bot.pending_updates[recordtype] = []
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

    async def dl_avys(self):
        self.logger.info('started avatar downloading task')

        async def url_to_bytes(hash, url):
            try:
                async with self.bot.session.get(str(url)) as r:
                    if r.status == 200:
                        await self.bot.avy_posting_queue.put((hash, BytesIO(await r.read())))
                        return
                    if r.status in [403, 404]:
                        # Discord has forsaken us. Mostly likely invalid url.
                        pass
                    elif r.status == 415:
                        # Discord is bad. retry with lower size.
                        url = URL(str(url))
                        new_size = int(url.query.get('size', 1024))//2
                        if new_size > 128:
                            # give up resizing it. Its too small to be worthwhile.
                            new_url = url.with_query(size=str(new_size))
                        else:
                            new_url = url.with_path(url.path.replace('gif','png')).with_query(size=1024)
                        self.bot.avy_urls[hash] = new_url
                    else:
                        # unsuccessful, put it back in for next round.
                        self.bot.avy_urls[hash] = url
                    self.logger.info(f'downloading {url} failed with {r.status}')
            except (asyncio.TimeoutError, aiohttp.ClientError):
                self.logger.exception(f'downloading {url} failed.')
                self.bot.avy_urls[hash] = url
        try:
            await self.bot.wait_until_ready()
            while True:
                while len(self.bot.avy_urls) == 0:
                    await asyncio.sleep(2)
                query = '''
                    select hash
                    from avy_urls
                    where
                        hash = any($1::text[])
                '''
                results = await self.bot.pool.fetch(query, self.bot.avy_urls.keys())
                for r in results:
                    # remove items in the avatar url dict that are already in the db
                    self.bot.avy_urls.pop(r['hash'], None)

                chunk = dict()
                while len(self.bot.avy_urls) > 0 and len(chunk) < (50 - self.bot.avy_posting_queue.qsize()):
                    # grabs enough avatars to fill the posting queue with 50 avatars if possible
                    avy, url = self.bot.avy_urls.popitem()
                    chunk[avy] = url
                if chunk:
                    await asyncio.gather(*[url_to_bytes(avy, url) for avy, url in chunk.items()])
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            self.logger.warning('avatar downloading task canceled')

    async def batch_post_avatars(self):
        self.logger.info('started avatar posting task')
        try:
            await self.bot.wait_until_ready()
            while True:
                while not self.wh:
                    self.wh = discord.utils.get(
                        await self.bot.get_guild(self.bot.avy_guild).webhooks(),
                        channel_id=self.bot.avy_channel
                    )
                    if self.wh:
                        self.logger.info(f'found webhook {self.wh.name} for {self.bot.avy_channel}')
                        break
                    else:
                        await asyncio.sleep(2)
                if self.bot.avy_posting_queue.qsize() == 0:
                    await asyncio.sleep(2)

                to_post = {}
                post_size = 0
                while len(to_post) < 10 and self.bot.avy_posting_queue.qsize() > 0:
                    avy, file = await self.bot.avy_posting_queue.get()
                    s = file.getbuffer().nbytes
                    if post_size + s < 8000000:
                        post_size += s
                        to_post[avy] = discord.File(file, filename=f'{avy}.{"png" if not avy.startswith("a_") else "gif"}')
                    elif s > 8000000:
                        new_bytes = None
                        if avy.startswith('a_'):
                            new_bytes = await self.bot.loop.run_in_executor(None, images.extract_first_frame, file)
                        else:
                            new_bytes = await self.bot.loop.run_in_executor(None, images.resize_to_limit, file, 8000000)
                        await self.bot.avy_posting_queue.put((avy, new_bytes))
                        continue
                    else:
                        await self.bot.avy_posting_queue.put((avy, file))
                        break
                if len(to_post) == 0:
                    continue

                backup = {k: BytesIO(v.fp.getbuffer()) for k, v in to_post.items()}

                for tries in range(5):
                    if tries > 0:
                        to_post = {k: discord.File(BytesIO(v.getbuffer()), filename=f'{k}.{"png" if not k.startswith("a_") else "gif"}') for k, v in backup.items()}
                    try:
                        message = await self.wh.send(content='\n'.join(to_post.keys()), wait=True, files=list(to_post.values()))
                        transformed = []
                        for a in message.attachments:
                            if a.height:
                                file_hash = os.path.splitext(a.filename)[0]
                                transformed.append(
                                    {
                                        'hash' : file_hash,
                                        'url' : a.url,
                                        'msgid' : message.id,
                                        'id' : a.id,
                                        'size' : a.size,
                                        'height' : a.height,
                                        'width' : a.width
                                    }
                                )
                                backup.pop(file_hash)
                        query = '''
                            insert into avy_urls
                            (hash, url, msgid, id, size, height, width)
                            select x.hash, x.url, x.msgid, x.id, x.size, x.height, x.width
                            from jsonb_to_recordset($1::jsonb) as x(hash text, url text, msgid bigint, id bigint, size bigint, height bigint, width bigint)
                            on conflict (hash) do nothing
                        '''
                        await self.bot.pool.execute(query, transformed)
                        if len(backup) == 0:
                            break
                        self.logger.warning(f'{len(backup)} failed to upload. retrying')
                    except discord.HTTPException:
                        self.logger.exception('something happened')
                    except aiohttp.ClientError:
                        self.logger.exception('discord big gay')
                    except ValueError:
                        self.logger.exception('for some reason the file is closed')
                    except TypeError:
                        self.logger.exception('for some reason discord api returned something empty')
                    await asyncio.sleep(2 + 2 * tries)

        except asyncio.CancelledError:
            self.logger.warning('Batching task for avatar posting was cancelled')

    async def first_sync(self):
        if self.bot.first_synced:
            return # Already successfully synced the bot after initial boot
        await self.bot.wait_until_ready()
        utcnow = datetime.datetime.utcnow()

        await self.bot.request_offline_members(*[guild for guild in self.bot.guilds if guild.large])
        await self.cog_log(True, utcnow - datetime.timedelta(microseconds=1))
        self.add_bulk_members(list(self.bot.get_all_members()), utcnow)
        self.synced.set()
        self.bot.first_synced = True
        self.logger.info("synced!")

    def add_bulk_members(self, members, utcnow):
        for m in members:
            self.bot.pending_updates['nicks'].append((m.id, m.guild.id, m.nick, utcnow))
        self.logger.info(f'Added members in bulk: {len(list(set(members)))}')
        for m in list(set(members)):
            self.bot.pending_updates['names'].append((m.id, m.name, utcnow))
            self.bot.pending_updates['avatars'].append((
                                                    m.id,
                                                    m.avatar if m.avatar else m.default_avatar.name,
                                                    utcnow
                                                  ))
            self.bot.pending_updates['discrims'].append((m.id, m.discriminator, utcnow))
            self.bot.pending_updates['statuses'].append((m.id, m.status.name, utcnow))
            self.bot.pending_updates['games'].append((m.id, m.activity.name if m.activity else None, utcnow))
            self.bot.avy_urls[m.avatar if m.avatar else m.default_avatar.name] = str(m.avatar_url_as(static_format='png'))


    def add_member(self, m, utcnow, full = True):
        self.bot.pending_updates['nicks'].append((m.id, m.guild.id, m.nick, utcnow))
        if full:
            self.bot.pending_updates['names'].append((m.id, m.name, utcnow))
            self.bot.pending_updates['avatars'].append((
                                                    m.id,
                                                    m.avatar if m.avatar else m.default_avatar.name,
                                                    utcnow
                                                  ))
            self.bot.pending_updates['discrims'].append((m.id, m.discriminator, utcnow))
            self.bot.pending_updates['statuses'].append((m.id, m.status.name, utcnow))
            self.bot.pending_updates['games'].append((m.id, m.activity.name if m.activity else None, utcnow))
            self.bot.avy_urls[m.avatar if m.avatar else m.default_avatar.name] = str(m.avatar_url_as(static_format='png'))

    def fill_updates(self, uid, sid, msg, utcnow, full = True):
        self.logger.info(f'running fill_updates with {full}')
        self.bot.pending_updates['nicks'].append((uid, sid, msg, utcnow))
        if full:
            self.bot.pending_removes.append((uid, utcnow))

    async def batch_member_remove(self):
        self.logger.info('started batch member remove task')
        try:
            await self.bot.wait_until_ready()
            while True:
                await asyncio.sleep(5)
                await self.insert_member_removes()
        except asyncio.CancelledError:
            self.logger.warning('task for batch member remove was cancelled')
            await self.insert_member_removes()
        self.logger.info('exited batch member remove task')

    async def insert_member_removes(self):
        to_insert = self.bot.pending_removes[:]
        if len(to_insert) == 0:
            return
        self.bot.pending_removes = []
        transformed = [{'uid' : row[0], 'time' : row[1]} for row in to_insert]
        query = f'''
                insert into member_removes (uid, time)
                select uid, time
                from jsonb_to_recordset($1::jsonb) as x(uid BIGINT, time TIMESTAMP WITHOUT TIME ZONE)
                '''
        await self.bot.pool.execute(query, transformed)

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
    async def on_user_update(self, before, after):
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        aid = after.id

        if before.name != after.name:
            self.bot.pending_updates['names'].append((aid, after.name, utcnow))
        if before.avatar != after.avatar:
            self.bot.pending_updates['avatars'].append((
                                                    aid,
                                                    after.avatar if after.avatar else after.default_avatar.name,
                                                    utcnow
                                                  ))
            self.bot.avy_urls[after.avatar if after.avatar else after.default_avatar.name] = str(after.avatar_url_as(static_format='png'))
        if before.discriminator != after.discriminator:
            self.bot.pending_updates['discrims'].append((aid, after.discriminator, utcnow))

    @commands.Cog.listener()
    async def on_member_update(self, before, after):
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        aid = after.id

        if before.nick != after.nick:
            self.bot.pending_updates['nicks'].append((aid, after.guild.id, after.nick, utcnow))

        lowest = discord.utils.find(lambda x: x.get_member(aid) is not None, sorted(self.bot.guilds, key=lambda x: x.id)) # stolen from luma I think
        
        if after.guild.id == lowest.id:
            if before.status != after.status:
                self.bot.pending_updates['statuses'].append((aid, after.status.name, utcnow))
            if before.activity != after.activity and not after.bot:
                self.bot.pending_updates['games'].append((aid, after.activity.name if after.activity else None, utcnow))

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """
            It is rare to have too many dups of members in new guilds.
            Regardless, dups don't matter and are easy to deal with.
        """
        await self.synced.wait()
        utcnow = datetime.datetime.utcnow()
        self.add_bulk_members(guild.members, utcnow)
        self.logger.info(f'Added {guild.member_count} people to queues!')

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
    if not hasattr(bot, 'pending_updates'):
        bot.pending_updates = {recordtype : [] for recordtype in scheme.keys()}
    if not hasattr(bot, 'pending_removes'):
        bot.pending_removes = []
    if not hasattr(bot, 'avy_urls'):
        bot.avy_urls = dict()
    if not hasattr(bot, 'avy_posting_queue'):
        bot.avy_posting_queue = asyncio.Queue(maxsize = 50)
    if not hasattr(bot, 'first_synced'):
        bot.first_synced = False

    bot.add_cog(Pop(bot))
