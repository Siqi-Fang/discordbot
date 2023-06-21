import discord
from discord.ext import commands, tasks
from discord.utils import get
from datetime import datetime, timedelta
from asgiref.sync import sync_to_async
import pytz
from utils import MetabaseService, NAME_2_TRACK, TRACKS
import pandas as pd
import os

@sync_to_async
def metabase():
    '''
    Creates Metabase service to recieve data.
    '''
    return MetabaseService()


async def get_summer_overall_data():
    '''
    Gets student rating for the entire program every day from Metabase
    '''
    daily_data = {}
    mb = await metabase()

    todays_reviews = 0
    yesterdays_reviews = 0

    today = await sync_to_async(mb.retrieve)(638)
    yesterday = await sync_to_async(mb.retrieve)(643)
    df_class_student = await sync_to_async(mb.retrieve)(640)

    df_today = pd.merge(today, df_class_student,
                  on='discordId', how='inner')
    df_yest = pd.merge(yesterday, df_class_student,
                       on='discordId', how='inner')

    yesterday_data = round(df_yest['rating'].mean(), 2)
    today_data = round(df_today['rating'].mean(), 2)

    # daily avg 
    avg_percent_change = round(
        (-(yesterday_data - today_data) / yesterday_data) * 100, 2)
    change_message = str(today_data) + '\nChange Since Yesterday: ' + \
        str(avg_percent_change) + '%'
    daily_data['Today\'s Average Rating'] = change_message

    #  review count
    todays_reviews = df_today['studentUsername'].count()
    yesterdays_reviews = df_yest['studentUsername'].count()
    count_percent_change = round(
        (-(yesterdays_reviews - todays_reviews) / yesterdays_reviews) * 100, 2)
    count_message = str(todays_reviews) + \
        '\nChange Since Yesterday: ' + str(count_percent_change) + '%'
    daily_data['Today\'s Number of Reviews'] = count_message

    # high ratings
    today_high = df_today[df_today['rating'] >= 9]['rating'].count()
    yesterday_high = df_yest[df_yest['rating'] >= 9]['rating'].count()

    high_percent_change = round(
        (-(yesterday_high - today_high) / yesterday_high) * 100, 2)
    high_message = str(today_high) + '\nChange Since Yesterday: ' + \
        str(high_percent_change) + '%'
    daily_data['Today\'s Percentage of 10 and 9 Reviews'] = high_message

    # low ratings
    today_low = df_today[df_today['rating'] <= 7]['rating'].count()
    yesterday_low = df_yest[df_yest['rating'] <= 7]['rating'].count()

    low_percent_change = round(
        (-(yesterday_low - today_low) / yesterday_low) * 100, 2)
    low_message = str(today_low) + '\nChange Since Yesterday: ' + \
        str(low_percent_change) + '%'
    daily_data['Today\'s Percentage of 7 or Less Reviews'] = low_message

    return daily_data


async def get_summer_group_data():
    '''
    Gets student rating for each group every day from Metabase
    '''
    daily_data = {}
    mb = await metabase()

    today = await sync_to_async(mb.retrieve)(638)
    df_class_student = await sync_to_async(mb.retrieve)(640)

    df = pd.merge(today, df_class_student,
                        on='discordId', how='inner')
    df['track'] = df['name'].map(NAME_2_TRACK)
    df = df[df['track'].notna()]

    for track in TRACKS:
        daily_data[track + ' Average Rating Today'] = round(
            df[df["track"]==track]["rating"].mean(), 2)
        
    return daily_data


intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)

@bot.command(name='groupreport')
#@commands.has_any_role(['AI Camp Avengers'])
async def get_summer_group_report(ctx):
    embed = discord.Embed()

    data = await get_summer_group_data()

    embed.title = 'Summer Group Ratings Report'
    embed.color = discord.Color.green()

    for key, value in data.items():
        embed.add_field(name=key, value=value, inline=False)

    return await ctx.send(embed=embed)


@bot.command(name='overallreport')
#@commands.has_any_role(['AI Camp Avengers'])
async def get_summer_overall_report(ctx):
    embed = discord.Embed()

    data = await get_summer_overall_data()

    embed.title = 'Summer Group Ratings Report'
    embed.color = discord.Color.green()

    for key, value in data.items():
        embed.add_field(name=key, value=value, inline=False)

    return await ctx.send(embed=embed)

@tasks.loop(hours=1)
async def auto_summer_group_report():
    '''
    Requests metabase data and automatically sends it at the end of the week
    '''
    date_utc = datetime.now(tz=pytz.UTC)
    date_pst = date_utc.astimezone(pytz.timezone('US/Pacific'))

    if date_pst.hour == 11: # CHANGE BACK TO 16 TO RE-ENABLE

        embed = discord.Embed()

        data = await get_summer_group_data()

        channel = discord.utils.get(bot.get_all_channels(), name='summer-camp')

        embed.title = ''
        embed.description = ''
        embed.color = discord.Color.green()

        for key, value in data.items():
            embed.add_field(name=key, value=value, inline=False)

        return await channel.send(embed=embed)

@tasks.loop(hours=1)
async def auto_summer_overall_report():
    '''
    Requests metabase data and automatically sends it at the end of the week
    '''
    date_utc = datetime.now(tz=pytz.UTC)
    date_pst = date_utc.astimezone(pytz.timezone('US/Pacific'))

    if date_pst.hour == 11: # CHANGE BACK TO 16 TO RE-ENABLE

        embed = discord.Embed()

        data = await get_summer_overall_data()

        channel = discord.utils.get(bot.get_all_channels(), name='summer-camp')

        embed.title = ''
        embed.description = ''
        embed.color = discord.Color.green()

        for key, value in data.items():
            embed.add_field(name=key, value=value, inline=False)

        return await channel.send(embed=embed)
    
@bot.event
async def on_ready():
    auto_summer_group_report.start()
    auto_summer_overall_report.start()

bot.run(os.environ["TOKEN"])
