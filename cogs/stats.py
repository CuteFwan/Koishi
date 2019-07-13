import discord
from discord.ext import commands
import time
import datetime
from io import BytesIO
from .utils import pretty
import typing
from math import cos, sin, radians, ceil
from PIL import Image, ImageOps, ImageDraw, ImageFilter, ImageEnhance, ImageFont
import logging

status = {'online':(67, 181, 129),
          'idle':(250, 166, 26),
          'dnd':(240, 71, 71),
          'offline':(116, 127, 141)}
discord_neutral = (188,188,188)

query_base = '''
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
            (select event, time
            from cog_log
            order by time desc)
            union
            (select 'guild_leave', time
            from member_removes
            where uid=$1
            order by time desc)
            union
            (select status, first_seen
            from statuses
            where uid=$1
            order by first_seen desc
            limit 3000)
        ) first3000
        order by first_seen_chopped desc, first_seen desc
    ) subtable
    where
        status is distinct from status_last
    order by first_seen desc
)
'''


class Stats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("koishi")
        
    @commands.command()
    async def useruptime(self, ctx, *, target : discord.Member = None):
        target = target or ctx.author
        if target.id == self.bot.user.id:
            return await ctx.send("I cannot see myself...")
        msg = f'`{target.display_name} `has been **{target.status.name}** for as long as I can tell...'
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
            msg = f'`{target.display_name} `has been **{target.status.name}** for {time}.'
                
            if target.status.name != 'offline':
                if offline_info:
                    time = pretty.delta_to_str(offline_info, utcnow)
                    msg2 = f'Last **offline** {time} ago.'
                else:
                    msg2 = 'Has not been seen offline in the last 30 days as far as I can tell...'
            
        await ctx.send(f'{msg}\n{msg2}')

    @commands.command()
    async def piestatus(self, ctx, *, target : discord.Member = None):
        '''Generates a pie chart displaying the ratios between the statuses the bot has seen the user use.'''
        target = target or ctx.author
        async with ctx.channel.typing():
            rows = await self.bot.pool.fetch(query_base + '''
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
            async with self.bot.session.get(str(target.avatar_url_as(format='png'))) as r:
                avydata = BytesIO(await r.read())
            statuses = {row['status'] : row['sum'] for row in rows if row['status'] in status.keys()}
            data = await self.bot.loop.run_in_executor(None, self._piestatus, avydata, statuses)
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
        by = {'online':60, 'idle':110, 'dnd':160, 'offline':210}
        base.paste(piebase, None, piebase)
        draw = ImageDraw.Draw(base)
        self.logger.debug(f'total statuses: {total}')
        for k, v in statuses.items():
            draw.rectangle(((bx, by[k]),(bx+30, by[k]+30)), fill=status[k], outline=(255,255,255,255))
            draw.text((bx+40, by[k]+8), f'{(v/total)*100:.2f}%', fill=discord_neutral, font=font)
            self.logger.debug(f'{(v/total)*100:.2f}%')
        del draw
        buffer = BytesIO()
        base.save(buffer, 'png')
        buffer.seek(0)
        return buffer

    @commands.command()
    async def barstatus(self, ctx, *, target : discord.Member = None):
        '''Generates a bar graph of each status the bot has seen the user use.'''
        target = target or ctx.author
        async with ctx.channel.typing():
            rows = await self.bot.pool.fetch(query_base + '''
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
            statuses = {row['status'] : row['sum'] for row in rows if row['status'] in status.keys()}
            data = await self.bot.loop.run_in_executor(None, self._barstatus, f'{target}\'s uptime in the past 30 days', statuses)
            await ctx.send(file=discord.File(data, filename=f'{target.display_name}_bar_status.png'))

    def _barstatus(self, title, statuses):
        highest = max(statuses.values())
        highest_unit = self.get_significant(highest)
        units = {stat:self.get_significant(value) for stat, value in statuses.items()}
        heights = {stat:(value/highest)*250 for stat, value in statuses.items()}
        box_size = (400,300)
        rect_x_start = {k:64 + (84 * v) for k, v in {'online':0,'idle':1,'dnd':2,'offline':3}.items()}
        rect_width = 70
        rect_y_end = 275
        labels = {'online':'Online', 'idle':'Idle', 'dnd':'DnD', 'offline':'Offline'}
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
    async def histostatus(self, ctx, target : typing.Optional[discord.Member] = None , tz : int = 0):
        if tz > 12 or tz < -12:
            tz = 0
        target = target or ctx.author
        query = query_base + '''
            select
                hour,
                status,
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
            current_hour = (utcnow.hour + tz) % 24
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
        with Image.open('histogram_template2.png') as base:
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

            first = {'online':0,'idle':0,'dnd':0,'offline':0}
            curr = {'online':0,'idle':0,'dnd':0,'offline':0}
            prev = {'online':0,'idle':0,'dnd':0,'offline':0}
            for d in data:
                if d['hour'] == 0:
                    first[d['status']] = d['percent']
                if d['hour'] == 23:
                    prev[d['status']] = d['percent']

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
                    curr = {'online':0,'idle':0,'dnd':0,'offline':0}
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

    @commands.command(aliases = ['hourlystatus'])
    async def calendarstatus(self, ctx, target : typing.Optional[discord.Member] = None , tz : int = 0):
        '''shows hourly presence data. Each row is a day. WIP'''
        if tz > 12 or tz < -12:
            tz = 0
        tz_delta = datetime.timedelta(hours=tz)
        target = target or ctx.author
        query = query_base + '''
            select
                s.hours as timestamp,
                extract(day from s.hours) as day,
                extract(hour from s.hours) as hour,
                s.status,
                sum(
                    extract(EPOCH from 
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
                    )/3600
                )as percent
            from (
                select
                    status,
                    first_seen + $2 as first_seen,
                    last_seen + $2 as last_seen,
                    generate_series(
                        date_trunc('hour', first_seen + $2),
                        date_trunc('hour', case when last_seen is null then now() at time zone 'utc' else last_seen end + $2),
                        '1 hours'
                    ) as hours
                from status_data
                where
                    status in ('offline', 'idle', 'online', 'dnd')
            ) as s
            group by timestamp, status
            order by timestamp, hour asc
            '''
        async with ctx.channel.typing():
            data = await ctx.bot.pool.fetch(query, target.id, tz_delta)
            output = await self.bot.loop.run_in_executor(None, self._calendarstatus, data, tz)
            await ctx.send(file=discord.File(output, filename='test.png'))

    def _calendarstatus(self, data, tz):
        base = Image.new(mode='RGBA', size=(24, 31), color=(0, 0, 0, 0))
        pix = base.load()
        status_percent = {}
        prev_timestamp = data[0]['timestamp']
        prev_day = data[0]['day']
        y = 0
        for d in data:
            if d['day'] != prev_day:
                y += (d['timestamp'].date() - prev_timestamp.date()).days
                prev_day = d['day']
            if prev_timestamp != d['timestamp']:
                x = d['hour']
                pix[x,y] = self._calculate_color(status_percent, status)
                prev_timestamp = d['timestamp']
                status_percent = {}
            status_percent[d['status']] = d['percent']

        base = base.crop((0,0,24,y+1))
        new_base = Image.new(mode='RGBA', size=(24, 31), color=(0, 0, 0, 0))
        new_base.paste(base, box=(0,30-y),mask=base)
        new_base = new_base.resize((400,new_base.size[1]),Image.NEAREST)
        new_base = new_base.resize((400,300),Image.NEAREST)

        buffer = BytesIO()
        new_base.save(buffer, 'png')
        buffer.seek(0)
        return buffer

    @commands.command()
    async def hourlyupdates(self, ctx, target : typing.Optional[discord.Member] = None , tz : int = 0):
        if tz > 12 or tz < -12:
            tz = 0
        tz_delta = datetime.timedelta(hours=tz)
        target = target or ctx.author
        query = query_base + '''
            select
                s.timestamp,
                extract(day from s.timestamp) as day,
                extract(hour from s.timestamp) as hour,
                count(s.timestamp)
            from (
                select
                    date_trunc('hour', first_seen + $2) as timestamp
                from status_data
                where
                    status in ('offline', 'idle', 'online', 'dnd')
            ) as s
            group by timestamp
            order by timestamp asc
            '''
        async with ctx.channel.typing():
            data = await ctx.bot.pool.fetch(query, target.id, tz_delta)
            output = await self.bot.loop.run_in_executor(None, self._hourlyupdates, data, tz)
            await ctx.send(file=discord.File(output, filename='test.png'))

    def _hourlyupdates(self, data, tz):
        base = Image.new(mode='RGBA', size=(24, 31), color=(0, 0, 0, 0))
        pix = base.load()
        prev_timestamp = data[0]['timestamp']
        prev_day = data[0]['day']
        y = 0
        for d in data:
            if d['day'] != prev_day:
                y += (d['timestamp'].date() - prev_timestamp.date()).days
                prev_timestamp = d['timestamp']
                prev_day = d['day']
            x = d['hour']
            amount = min(1, d['count']/30)
            overload = min(1, max(0, (d['count'] - 30)/30))
            amount -= overload
            percents = {'activity' : amount, 'overload' : overload}
            colors = {'activity' : (67, 181, 129), 'overload' : (255,255,255)}
            pix[x,y] = self._calculate_color(percents, colors)

        base = base.crop((0,0,24,y+1))
        new_base = Image.new(mode='RGBA', size=(24, 31), color=(0, 0, 0, 0))
        new_base.paste(base, box=(0,30-y),mask=base)
        new_base = new_base.resize((400,new_base.size[1]),Image.NEAREST)
        new_base = new_base.resize((400,300),Image.NEAREST)

        buffer = BytesIO()
        new_base.save(buffer, 'png')
        buffer.seek(0)
        return buffer

    def _calculate_color(self, percent, colors):
        mult = sum(percent.values())
        new_color = [int(sum((percent[key] / mult) * colors[key][i] for key, value in percent.items())) for i in range(3)]
        alpha = ceil(mult * 255)
        color_with_alpha = tuple([*new_color, alpha])
        return color_with_alpha

    @commands.command()
    @commands.cooldown(1,7200, commands.BucketType.user)
    async def getstatusdata(self, ctx, limit : int = 0):
        async with ctx.channel.typing():
            buf = BytesIO()
            query = f'''
                select
                    status,
                    first_seen
                from statuses
                where uid=$1
                order by first_seen desc
                {f'limit {limit}' if limit > 0 else ''}
            '''
            async with self.bot.pool.acquire() as con:
                await con.copy_from_query(query, ctx.author.id, output=buf, format='csv')
            buf.seek(0)
            await ctx.send(file=discord.File(buf, filename=f'{ctx.author.id}_statuses.csv'))


def setup(bot):
    bot.add_cog(Stats(bot))
