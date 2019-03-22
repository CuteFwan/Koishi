import discord
from discord.ext import commands
import time
import datetime
from io import BytesIO
from .utils import pretty
import asyncio
import aiohttp
import json
import typing
from math import cos, sin, radians, ceil
from PIL import Image, ImageOps, ImageDraw, ImageFilter, ImageEnhance, ImageFont

status = {'online':(67, 181, 129),
          'away':(250, 166, 26),
          'dnd':(240, 71, 71),
          'offline':(116, 127, 141)}
discord_neutral = (188,188,188)

class Stats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
    @commands.command()
    async def myuptime(self, ctx, target : discord.Member = None):
        target = target or ctx.author
        if target.id == self.bot.user.id:
            return await ctx.send("I cannot see myself...")
        msg = f'{target.display_name} has been **{target.status.name}** for as long as I can tell...'
        msg2 = ''
        status_info = offline_info = None
        status_info = await self.bot.pool.fetchval('''
            with lagged as(
                select
                    status,
                    lag(status) over (order by first_seen asc) as status_lag,
                    first_seen
                from statuses
                where (uid=$1 or uid=0) and
                    first_seen > now() at time zone 'utc' - interval '30 days'
            )
            select distinct on (status)
                first_seen
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
                        first_seen
                    from statuses
                    where (uid=$1 or uid=0) and
                        first_seen > now() at time zone 'utc' - interval '30 days'
                )
                select
                    first_seen
                from lagged
                where
                    status != 'offline' and status_lag = 'offline'
                order by first_seen desc
                limit 1
            ''', target.id)
             
        if status_info:
            utcnow = datetime.datetime.utcnow()
            time = pretty.delta_to_str(status_info, utcnow)
            msg = f'{target.display_name} has been **{target.status.name}** for {time}.'
                
            if target.status.name != 'offline':
                if offline_info:
                    time = pretty.delta_to_str(offline_info, utcnow)
                    msg2 = f'Last **offline** {time} ago.'
                else:
                    msg2 = 'Has not been seen offline in the last 30 days as far as I can tell...'

            
        await ctx.send(f'{msg}\n{msg2}')

    @commands.command()
    @commands.is_owner()
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
                        from  ( 
                            (select status, first_seen
                            from statuses
                            where uid=0
                            order by first_seen desc)
                            union all
                            (select status, first_seen
                            from statuses
                            where uid=$1
                            order by first_seen desc
                            limit 2000)
                        ) first2000
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
        with Image.open(avydata).resize((200,200), resample=Image.BICUBIC).convert('RGBA') as avy:
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
                    mult = 1000
                    offset = 150
                    for k, v in angles.items():
                        x = offset + ceil(offset * mult * cos(radians(v))) / mult
                        y = offset + ceil(offset * mult * sin(radians(v))) / mult
                        draw.line(((offset, offset), (x, y)), fill=(255,255,255,255), width=1)
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


    @commands.command()
    @commands.is_owner()
    async def barstatus(self, ctx, target : discord.Member = None):
        '''Generates a bar graph of each status the bot has seen the user use.'''
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
                        from  ( 
                            (select status, first_seen
                            from statuses
                            where uid=0
                            order by first_seen desc)
                            union all
                            (select status, first_seen
                            from statuses
                            where uid=$1
                            order by first_seen desc
                            limit 2000)
                        ) first2000
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
            data = await self.bot.loop.run_in_executor(None, self._barstatus, f'{target}\'s uptime in the past 30 days', data)
            await ctx.send(file=discord.File(data, filename=f'{target.display_name}_bar_status.png'))
    def _barstatus(self, title, statuses):
        highest = max(statuses.values())
        highest_unit = self.get_significant(highest)
        units = {stat:self.get_significant(value) for stat, value in statuses.items()}
        heights = {stat:(value/highest)*250 for stat, value in statuses.items()}
        box_size = (400,300)
        rect_x_start = {k:64 + (84 * v) for k, v in {'online':0,'away':1,'dnd':2,'offline':3}.items()}
        rect_width = 70
        rect_y_end = 275
        labels = {'online':'Online', 'away':'Away', 'dnd':'DnD', 'offline':'Offline'}
        base = Image.new(mode='RGBA', size=box_size, color=(0, 0, 0, 0))
        with Image.open('barstatus_grid1.png') as grid:
            font = ImageFont.truetype("arial.ttf", 12)
            draw = ImageDraw.Draw(base)
            draw.text((5, 5), highest_unit[1], fill=discord_neutral, font=font)
            draw.text((52,2),title, fill=discord_neutral,font=font)
            divs = 11
            for i in range(divs):
                draw.line(((50,25+((box_size[1]-50)/(divs-1))*i),(box_size[0],25+((box_size[1]-50)/(divs-1))*i)),fill=(*discord_neutral,128), width=1)
                draw.text((5, 25+((box_size[1]-50)/(divs-1))*i-6), f'{highest_unit[0]-i*highest_unit[0]/(divs-1):.2f}', fill=discord_neutral, font=font)
            for k, v in statuses.items():
                draw.rectangle(((rect_x_start[k], rect_y_end - heights[k]),(rect_x_start[k]+rect_width, rect_y_end)), fill=status[k])
                draw.text((rect_x_start[k], rect_y_end - heights[k] - 13), f'{units[k][0]} {units[k][1]}', fill=discord_neutral, font=font)
                draw.text((rect_x_start[k], box_size[1] - 25), labels[k], fill=discord_neutral, font=font)
            del draw
            base.paste(grid, None, grid)
        buffer = BytesIO()
        base.save(buffer, 'png')
        buffer.seek(0)
        return buffer
    def get_significant(self, stat):
        word = ''
        if stat >= 604800:
            stat /= 604800
            word = 'Week' 
        elif stat >= 86400:
            stat /= 86400
            word = 'Day' 
        elif stat >= 3600:
            stat /= 3600
            word = 'Hour' 
        elif stat >= 60:
            stat /= 60
            word = 'Minute' 
        else:
            word = 'Second'
        stat = float(f'{stat:.1f}')
        if stat > 1 or stat == 0.0:
            word += 's'
        return stat, word

    @commands.command()
    @commands.is_owner()
    async def histostatus(self, ctx, target : typing.Optional[discord.Member] = None , tz : int = 0):
        if tz > 12 or tz < -12:
            tz = 0
        target = target or ctx.author
        query = '''
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
                    from ( 
                        (select status, first_seen
                        from statuses
                        where uid=0
                        order by first_seen desc)
                        union all
                        (select status, first_seen
                        from statuses
                        where uid=$1
                        order by first_seen desc
                        limit 2000)
                    ) first2000
                    order by first_seen_chopped desc, first_seen desc
                ) subtable
                where
                    status is distinct from status_last
                order by first_seen desc
            )
            select
                hour,
                case when status = 'idle' then 'away' else status end,
                extract(epoch from total) / extract(epoch from max(total) over ()) as percent
            from (
                select
                    mod((extract(hour from s.hours)+$2+24)::integer, 24) as hour,
                    s.status,
                    sum(
                    case 
                        when date_trunc('hour', s.last_seen) = s.hours and
                             date_trunc('hour', s.first_seen) = s.hours then
                             s.last_seen - s.first_seen
                        when date_trunc('hour', s.first_seen) = s.hours then 
                             (s.hours + interval '1 hour') - s.first_seen
                        when date_trunc('hour', s.last_seen) = s.hours then
                             s.last_seen - s.hours
                        else
                            interval '1 hour'
                    end
                    ) as total
                from (
                    select
                        status,
                        first_seen,
                        last_seen,
                        generate_series(
                            date_trunc('hour', first_seen),
                            date_trunc('hour', case when last_seen is null then now() at time zone 'utc' else last_seen end),
                            '1 hours'
                        ) as hours
                    from status_data
                    where
                        status in ('offline', 'idle', 'online', 'dnd')
                ) as s
                group by hour, s.status
                order by hour asc
            ) a
            order by hour asc
            '''
        async with ctx.channel.typing():
            utcnow = datetime.datetime.utcnow()
            start_time = time.perf_counter()
            data = await self.bot.pool.fetch(query, target.id, tz)
            query_done_time = time.perf_counter()
            current_hour = utcnow.hour + tz
            output = await self.bot.loop.run_in_executor(None, self._histostatus, f'{target.display_name}\'s resturant hours', data, current_hour, tz)
            generated_time = time.perf_counter()
            await ctx.send(file=discord.File(output, filename=f'{target.id} histostatus {utcnow.replace(microsecond=0,second=0,minute=0)}.png'))
            finish_time = time.perf_counter()
            msg = f'query done in **{(query_done_time - start_time)*1000:.2f}ms**'
            msg += f'\nimage built in **{(generated_time - query_done_time)*1000:.2f}ms**'
            msg += f'\nsent image in **{(finish_time - generated_time)*1000:.2f}ms**'
            msg += f'\ntotal time **{(finish_time - start_time)*1000:.2f}ms**'
            await ctx.send(f'*{msg}*')
        
    def _histostatus(self, title, data, current_hour, tz):
        box_size = (400,300)
        #base = Image.new(mode='RGBA', size=box_size, color=(0, 0, 0, 0))
        with Image.new(mode='RGBA',size=box_size) as base:
            draw = ImageDraw.Draw(base)
            x = 24
            spacing = 16
            draw_y0 = 0
            draw_y1 = box_size[1]-30
            trans_font = (*discord_neutral, 50)
            font = ImageFont.truetype("arial.ttf", 12)
            graphsize = 255
            top_offset = 15
            for i in range(25):
                #Draw numbers
                draw_x = x+spacing*i-8
                draw.line(((draw_x,draw_y0),(draw_x,draw_y1)),fill=trans_font, width=1)
                draw.line(((draw_x, draw_y1), (draw_x, draw_y1+top_offset)), fill=discord_neutral, width=1)
                if i != 24:
                    if i == current_hour:
                        fontcolor = (0,255,0,255)
                    else:
                        fontcolor = discord_neutral
                    draw.text((draw_x+1,draw_y1), f'{i:02}', fill=fontcolor, font=font)
            draw.text((340,draw_y1+16), f'{"+" if tz >= 0 else ""}{tz}', fill=discord_neutral, font=font)
            draw.text((2,2),title, fill=discord_neutral,font=font)

            first = {'online':0,'away':0,'dnd':0,'offline':0}
            for d in data:
                if d['hour'] == 0:
                    first[d['status']] = d['percent']
                else:
                    break
            prev = {'online':0,'away':0,'dnd':0,'offline':0}
            for d in data[::-1]:
                if d['hour'] == 23:
                    prev[d['status']] = d['percent']
                else:
                    break

            curr = {'online':0,'away':0,'dnd':0,'offline':0}
            hour = 0
            for d in data:
                if hour == d['hour']:
                    curr[d['status']] = d['percent']
                elif hour + 1 == d['hour']:
                    for stat in prev.keys():
                        x0 = x - spacing
                        y0 = (graphsize - (prev[stat]*graphsize)) + top_offset
                        x1 = x
                        y1 = (graphsize - (curr[stat]*graphsize)) + top_offset
                        draw.line(((x0,y0),(x1,y1)), fill=status[stat], width=1)
                        draw.ellipse(((x1-1,y1-1),(x1+1,y1+1)), fill=status[stat])
                    prev = curr
                    curr = {'online':0,'away':0,'dnd':0,'offline':0}
                    curr[d['status']] = d['percent']
                    hour += 1
                    x += spacing
            for k, v in prev.items():
                x0 = x - spacing
                y0 = (graphsize - v*graphsize) + top_offset
                x1 = x
                y1 = (graphsize - first[k]*graphsize) + top_offset
                draw.line(((x0,y0),(x1,y1)), fill=status[k], width=1)

            del draw
            buffer = BytesIO()
            base.save(buffer, 'png')
        buffer.seek(0)
        return buffer

def setup(bot):
    bot.add_cog(Stats(bot))