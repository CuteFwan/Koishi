import discord
from discord.ext import commands
import time
import math
import asyncio
import datetime
from PIL import Image
from io import BytesIO
from .utils import images

class Avatar(commands.Cog):
    def __init__(self, bot):
        self.bot = bot


    @commands.command()
    async def avyquilt(self, ctx, member : discord.Member = None):
        async with ctx.channel.typing():
            member = member or ctx.author
            query = '''
                select
                    avy_urls.url
                from koi.avatars
                left join koi.avy_urls on
                    avy_urls.hash = avatars.avatar
                where
                    avatars.uid = $1
                order by avatars.first_seen desc
            '''

            start_time = time.perf_counter()

            urls = await ctx.bot.pool.fetch(query, member.id)

            query_time = time.perf_counter()
            print(f'{(query_time - start_time)*1000:.2f}ms to query')

            async def url_to_bytes(url):
                if not url:
                    return None
                async with ctx.bot.session.get(url) as r:
                    return BytesIO(await r.read())

            avys = await asyncio.gather(*[url_to_bytes(url['url']) for url in urls])

            dl_time = time.perf_counter()
            print(f'{(dl_time - query_time)*1000:.2f}ms to dl avatars')

            file = await ctx.bot.loop.run_in_executor(None, self._avyquilt, avys)

            write_time = time.perf_counter()
            print(f'{(write_time - dl_time)*1000:.2f}ms to write file')

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