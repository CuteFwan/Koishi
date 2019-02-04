import discord
from discord.ext import commands
import imp
import time
import datetime
from io import BytesIO
import asyncio
import aiohttp
import json
from numpy import cos, sin, radians, ceil
from PIL import Image, ImageOps, ImageDraw, ImageFilter, ImageEnhance, ImageFont

status = {'online':(67, 181, 129),
          'away':(250, 166, 26),
          'dnd':(240, 71, 71),
          'offline':(116, 127, 141)}
discord_neutral = (188,188,188)

class Stats:
    def __init__(self, bot):
        self.bot = bot
        
    @commands.command()
    async def myuptime(self, ctx, target : discord.Member = None):
        target = target or ctx.author
        msg = 'Not enough information.'
        status_info = offline_info = None
        status_info = await self.bot.pool.fetchval('''
            with lagged as(
                select
                    status,
                    lag(status) over (order by first_seen asc) as status_lag,
                    first_seen,
                    now() at time zone 'utc' - first_seen as since
                from statuses
                where (uid=$1 or uid=0) and
                    first_seen > now() at time zone 'utc' - interval '30 days'
            )
            select distinct on (status)
                since
            from lagged
            where 
                status != status_lag and
                status = $2
            order by status, first_seen desc
        ''', target.id, target.status.name)
        
        if target.status.name != 'offline':
            offline_info = await self.bot.pool.fetchval('''
                with lagged as(
                    select
                        status,
                        lag(status) over (order by first_seen asc) as status_lag,
                        first_seen,
                        now() at time zone 'utc' - first_seen as since
                    from statuses
                    where (uid=$1 or uid=0) and
                        first_seen > now() at time zone 'utc' - interval '30 days'
                )
                select
                    since
                from lagged
                where
                    status != 'offline' and status_lag = 'offline'
                order by since asc
                limit 1
            ''', target.id)
             
        if status_info:   
            msg = f'{target.display_name} has been **{target.status.name}** for '
            d, s = divmod(int(status_info.total_seconds()), 86400)
            h, s = divmod(s, 3600)
            m, s = divmod(s, 60)
            if d != 0:
                msg += '{}d {}h {}m'.format(d, h, m)
            elif h != 0:
                msg += '{}h {}m'.format(h, m)
            else:
                msg += '{}m'.format(m)
                
            if offline_info:
                msg += '\nLast **offline** '
                d, s = divmod(int(offline_info.total_seconds()), 86400)
                h, s = divmod(s, 3600)
                m, s = divmod(s, 60)
                if d != 0:
                    msg += '{}d {}h {}m'.format(d, h, m)
                elif h != 0:
                    msg += '{}h {}m'.format(h, m)
                else:
                    msg += '{}m'.format(m)
                msg += ' ago.'
            else:
                msg += '\nHas not been seen offline in the last 30 days.'
            
        await ctx.send(msg)

    @commands.command()
    async def piestatus(self, ctx, target : discord.Member = None):
        '''Generates a pie chart displaying the ratios between the statuses the bot has seen the user use.'''
        target = target or ctx.author
        async with ctx.channel.typing():
            rows = await self.bot.pool.fetch('''
                with status_data as(
                    select
                        status,
                        first_seen_chopped as first_seen,
                        case when 
                            lag(first_seen_chopped) over (order by first_seen desc) is null then
                                now() at time zone 'utc'
                            else
                                lag(first_seen_chopped) over (order by first_seen desc)
                        end as last_seen
                    from (
                        select
                            distinct on (first_seen_chopped)
                            first_seen,
                            case when first_seen < (now() at time zone 'utc' - interval '30 days') then
                                (now() at time zone 'utc' - interval '30 days')
                                else first_seen end as first_seen_chopped,
                            status,
                            lag(status) over (order by first_seen desc) as status_last
                        from statuses
                        where uid=$1 or uid=0
                        order by first_seen_chopped desc, first_seen desc
                    ) subtable
                    where
                        status is distinct from status_last
                    order by first_seen desc
                )
                select
                    status,
                    sum(
                    extract(
                    epoch from(
                        last_seen - first_seen
                    ))) as sum
                from status_data
                group by status
                order by sum desc
            ''', target.id)
            async with self.bot.session.get(target.avatar_url_as(format='png')) as r:
                avydata = BytesIO(await r.read())
            data = dict()
            for row in rows:
                if row['status'] == 'online':
                    data['online'] = row['sum']
                elif row['status'] == 'offline':
                    data['offline'] = row['sum']
                elif row['status'] == 'idle':
                    data['away'] = row['sum']
                elif row['status'] == 'dnd':
                    data['dnd'] = row['sum']
            data = await self.bot.loop.run_in_executor(None, self._piestatus, avydata, data)
            await ctx.send(file=discord.File(data, filename=f'{target.display_name}_pie_status.png'))
    def _piestatus(self, avydata, statuses):
        total = sum(statuses.values())
        stat_deg = {k:(v/total)*360 for k, v in statuses.items()}
        angles = dict()
        starting = -90
        for k,v in stat_deg.items():
            angles[k] = starting + v
            starting += v
        base = Image.new(mode='RGBA', size=(400, 300), color=(0, 0, 0, 0))
        piebase = Image.new(mode='RGBA', size=(400, 300), color=(0, 0, 0, 0))
        with Image.open(avydata).resize((200,200), resample=Image.BILINEAR).convert('RGBA') as avy:
            with Image.open('piestatustest2.png').convert('L') as mask:
                base.paste(avy, (50,50), avy)
                draw = ImageDraw.Draw(piebase)
                maskdraw = ImageDraw.Draw(mask)
                starting = -90
                for k, v in angles.items():
                    if starting == v:
                        continue
                    else:
                        draw.pieslice(((-5,-5),(305,305)),starting, v, fill=status[k])
                        starting = v
                if not 360 in stat_deg:
                    for k, v in angles.items():
                        x = 150 + ceil(15000 * cos(radians(v)))/100
                        y = 150 + ceil(15000 * sin(radians(v)))/100
                        draw.line(((150, 150), (x, y)), fill=(255,255,255,255), width=1)
                del maskdraw
                piebase.putalpha(mask)
        font = ImageFont.truetype("arial.ttf", 15)
        bx = 310
        by = {'online':60, 'away':110, 'dnd':160, 'offline':210}
        base.paste(piebase, None, piebase)
        draw = ImageDraw.Draw(base)
        print(total)
        for k, v in statuses.items():
            draw.rectangle(((bx, by[k]),(bx+30, by[k]+30)), fill=status[k], outline=(255,255,255,255))
            draw.text((bx+40, by[k]+8), f'{(v/total)*100:.2f}%', fill=discord_neutral, font=font)
            print(f'{(v/total)*100:.2f}%')
        del draw
        buffer = BytesIO()
        base.save(buffer, 'png')
        buffer.seek(0)
        return buffer
        
def setup(bot):
    bot.add_cog(Stats(bot))