import asyncio
import discord
from discord import Webhook
import aiohttp

class PromAPI:
    """
    Returns data from the metrics endpoints
    """
    pass

class LogsAPI:
    """
    Returns data contained in the logs so users can monitor events
    """
    pass

class SystemAPI:
    """
    Returns data about the system so users can monitor usage
    """
    pass

class DiscordAPI:

    @staticmethod
    async def send_message(url, message):
        async with aiohttp.ClientSession() as session:
            webhook = Webhook.from_url(url, session=session)
            embed = discord.Embed(
                title="New Reward!",
                description=message,
                color=discord.Color.green()
            )
            await webhook.send(embed=embed)

    def send_discord_message(url, message):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            DiscordAPI.send_message(url, message)
        )
        loop.close()