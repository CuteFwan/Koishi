import logging
from discord.ext import commands
import time
import io
import inspect
import traceback
import textwrap
from contextlib import redirect_stdout
from .utils import pretty

logger = logging.getLogger(__name__)

class Admin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._last_result = None

    async def cog_check(self, ctx):
        return ctx.author.id in self.bot.admins

    @commands.command(hidden=True)
    async def load(self, ctx, extension_name: str):
        ctx.bot.load_extension(extension_name)
        logger.info(f'{extension_name} loaded.')
        await ctx.send(f'{extension_name} loaded.')

    @commands.command(hidden=True)
    async def unload(self, ctx, extension_name: str):
        ctx.bot.unload_extension(extension_name)
        logger.info(f'{extension_name} unloaded.')
        await ctx.send(f'{extension_name} unloaded.')

    @commands.command(hidden=True)
    async def reload(self, ctx, extension_name: str):
        ctx.bot.reload_extension(extension_name)
        logger.info(f'{extension_name} reloaded.')
        await ctx.send(f'{extension_name} reloaded.')

    @commands.command(hidden=True)
    async def sql(self, ctx, *, query):
        """Copied from old bot"""
        start = time.perf_counter()
        rows = await self.bot.pool.fetch(query)
        mid = time.perf_counter()
        data = []
        if len(rows) > 0:
            data.append([r for r in rows[0].keys()])
            data.extend([[r for r in row.values()] for row in rows])
            table = await pretty.tabulate(data, max=100)
            end = time.perf_counter()
            await ctx.send(f'```\n{table}```\n*got {len(rows)} row{"s" if len(rows) > 1 else ""} in {(mid-start)*1000:.2f}ms\nbuilt table in {(end-mid)*1000:.2f}ms*')
        else:
            end = time.perf_counter()
            await ctx.send(f'*got 0 rows in {(mid-start)*1000:.2f}ms*')

    @commands.command(hidden=True)
    async def eval(self, ctx, *, code : str):
        """Copied from old bot"""
        code = code.strip('` ')
        python = '```py\n{}\n```'

        env = {
            'bot': self.bot,
            'ctx': ctx,
            'message': ctx.message,
            'guild': ctx.guild,
            'channel': ctx.channel,
            'author': ctx.author,
            'self': self
        }
        env.update(globals())
        try:
            result = eval(code, env)
            if inspect.isawaitable(result):
                result = await result
        except Exception as e:
            await ctx.send(python.format(type(e).__name__ + ': ' + str(e)))
            return
        await ctx.send(content=python.format(result))

    def cleanup_code(self, content):
        """Automatically removes code blocks from the code."""
        # remove ```py\n```
        if content.startswith('```') and content.endswith('```'):
            return '\n'.join(content.split('\n')[1:-1])

        # remove `foo`
        return content.strip('` \n')
    
    def get_syntax_error(self, e):
        if e.text is None:
            return '```py\n{0.__class__.__name__}: {0}\n```'.format(e)
        return '```py\n{0.text}{1:>{0.offset}}\n{2}: {0}```'.format(e, '^', type(e).__name__)
    
    @commands.command(hidden=True)
    async def py(self, ctx, *, body: str):
        """Copied from old bot"""
        env = {
            'bot': self.bot,
            'ctx': ctx,
            'channel': ctx.channel,
            'author': ctx.author,
            'guild': ctx.guild,
            'message': ctx.message,
            '_': self._last_result
        }

        env.update(globals())
        body = self.cleanup_code(body)
        stdout = io.StringIO()

        to_compile = 'async def func():\n%s' % textwrap.indent(body, '  ')

        try:
            exec(to_compile, env)
        except SyntaxError as e:
            return await ctx.send(self.get_syntax_error(e))

        func = env['func']
        try:
            with redirect_stdout(stdout):
                ret = await func()
        except Exception as e:
            value = stdout.getvalue()
            await ctx.send('```py\n{}{}\n```'.format(value, traceback.format_exc()))
        else:
            value = stdout.getvalue()
            try:
                await ctx.message.add_reaction('\u2705')
            except:
                pass

            if ret is None:
                if value:
                    await ctx.send('```py\n%s\n```' % value)
            else:
                self._last_result = ret
                await ctx.send('```py\n%s%s\n```' % (value, ret))

    
def setup(bot):
    bot.add_cog(Admin(bot))
