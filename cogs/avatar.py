import discord
from discord.ext import commands
import time
import math
import asyncio
import datetime
from PIL import Image
from io import BytesIO
from .utils import images

class Timetracker:
    def __init__(self, title, *values):
        self.title = title
        self.values = values
        self.times = [time.perf_counter()]
    
    def update(self):
        self.times.append(time.perf_counter())

    def display(self):
        msg = self.title
        for i, value in enumerate(self.values):
            msg += f'\n{value}: {f"{(self.times[i+1] - self.times[i])*1000:.2f}ms" if i+1 < len(self.times) else "..."}'
        return msg

class Avatar(commands.Cog):
    def __init__(self, bot):
        self.bot = bot


    @commands.command()
    async def avyquilt(self, ctx, member : discord.Member = None):
        member = member or ctx.author

        query = '''
            select
                avy_urls.url
            from (
                select
                    avatar, first_seen
                from (
                    select
                        avatar, lag(avatar) over (order by first_seen desc) as avatar_old, first_seen
                    from koi.avatars
                    where
                        avatars.uid = $1
                ) a
                where
                    avatar != avatar_old or avatar_old is null
            ) avys
            left join koi.avy_urls on
                avy_urls.hash = avys.avatar
            order by avys.first_seen desc
        '''

        tracker = Timetracker('', 'queried', 'downloaded', 'created file')
        msg = await ctx.send(tracker.display())

        urls = await ctx.bot.pool.fetch(query, member.id)

        tracker.update()
        await msg.edit(content=tracker.display())

        async def url_to_bytes(url):
            if not url:
                return None
            async with ctx.bot.session.get(url) as r:
                return BytesIO(await r.read())

        avys = await asyncio.gather(*[url_to_bytes(url['url']) for url in urls])

        tracker.update()
        await msg.edit(content=tracker.display())

        file = await ctx.bot.loop.run_in_executor(None, self._avyquilt, avys)

        tracker.update()
        await msg.edit(content=tracker.display())

        await ctx.send(file=discord.File(file, f'{member.id}_avyquilt.png'))

    def _avyquilt(self, avatars):
        xbound = math.ceil(math.sqrt(len(avatars)))
        ybound = math.ceil(len(avatars) / xbound)
        size = int(2520 / xbound)

        with Image.new('RGBA', size=(xbound * size, ybound * size), color=(0,0,0,0)) as base:
            x, y = 0, 0
            for avy in avatars:
                if avy:
                    im = Image.open(avy).resize((size,size), resample=Image.BICUBIC)
                    base.paste(im, box=(x * size, y * size))
                if x < xbound - 1:
                    x += 1
                else:
                    x = 0
                    y += 1
            buffer = BytesIO()
            base.save(buffer, 'png')
            buffer.seek(0)
            buffer = images.resize_to_limit(buffer, 8000000)
            return buffer


    
def setup(bot):
    bot.add_cog(Avatar(bot))