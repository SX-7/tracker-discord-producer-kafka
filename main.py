# General idea:
# - Estabilish connectivity between two servers
# - Convert+send messages over to Kafka server
# In order to limit the boilerplate on producer side, decrease data size, we're only sending basic info, and standardazing it whenever we can
# It is up to consumer to create meaningful messages in UI from data from Kafka server
# TODO: More asynchronicity
# Maybe it's asynchronous enough, but it's always worth looking at different possibilities

from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
from dotenv import load_dotenv
import os
import discord
from typing import Union, Any
from pytermgui import tim

# load data
load_dotenv()

server_ip = os.getenv("KAFKA_SERVER_IP")
bot_token = os.getenv("DISCORD_BOT_TOKEN")
kafka_username = os.getenv("KAFKA_USERNAME")
kafka_password = os.getenv("KAFKA_PASSWORD")

# init connection
producer = KafkaProducer(bootstrap_servers=[str(server_ip)],
                         retries=5,
                         value_serializer=msgpack.dumps,
                         key_serializer=msgpack.dumps,
                         sasl_mechanism="PLAIN",
                         security_protocol="SASL_PLAINTEXT",
                         sasl_plain_username=kafka_username,
                         sasl_plain_password=kafka_password
                         )

if producer.bootstrap_connected():
    tim.print("[blue]INIT[/]    Estabilished connection with Kafka server")

# default logging config for discord.py will print info on success
client = discord.Client(intents=discord.Intents.all())


def send_message(producer: KafkaProducer, message_key: str, message_data: dict[str, Any]) -> None:
    # helper function for sending data
    future = producer.send(topic=f'discord-{kafka_username}-all', value=message_data,
                           key=message_key)
    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
        # To be restructured
        print("%s:%d:%d" % (record_metadata.topic,
                            record_metadata.partition, record_metadata.offset))
    except KafkaError as err:
        # Decide what to do if produce request failed...
        print(err.args)
        pass


def extract_message_data(message: discord.Message) -> dict[str, Any]:
    return {
        "id": message.id,
        "author": {"name": str(message.author), "id": message.author.id},
        "content": message.clean_content if message.type is discord.MessageType.default or message.type is discord.MessageType.reply else message.system_content,
        "channel": {"name": message.channel.name, "id": message.channel.id},
        "created": message.created_at.isoformat(' ', timespec='seconds'),
        "edited": message.edited_at.isoformat(' ', timespec='seconds') if message.edited_at is not None else None,
        "url": message.jump_url,
        "type": message.type,
        "attachments": [{"name": attachment.filename, "url": attachment.url} for attachment in message.attachments],
        "stickers": [{"name": sticker.name, "url": sticker.url}for sticker in message.stickers],
        "reference": {"id": message.reference.message_id, "url": message.reference.jump_url} if message.reference is not None else None
    }


def extract_reaction_data(reaction: discord.Reaction) -> dict[str, Any]:
    return {
        "name": reaction.emoji if isinstance(reaction.emoji, str) else reaction.emoji.name,
        "count": reaction.count,
        "url":  None if isinstance(reaction.emoji, str) else reaction.emoji.url
    }


@client.event
# useful paste-in from documentiation
async def on_ready():
    tim.print(f'[blue]INIT[/]   We have logged in as {client.user}')


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        # safety feature, though if it ever gets used, you likely messed up the bot
        return
    message_data = {
        "action_id": 1,
        "message": extract_message_data(message=message)
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=message.guild.id)


@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if after.edited_at is None:
        # sad neccesity due to weird discord embed handling
        return
    message_data = {
        "action_id": 2,
        "before": extract_message_data(before),
        "after": extract_message_data(after)
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=before.guild.id)


@client.event
async def on_message_delete(message: discord.Message):
    message_data = {
        "action_id": 3,
        "message": extract_message_data(message)
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=message.guild.id)


@client.event
async def on_bulk_message_delete(messages: list[discord.Message]):
    message_data = {
        "action_id": 4,
        "messages": [extract_message_data(message)for message in messages]
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=messages[0].guild.id)


@client.event
async def on_reaction_add(reaction: discord.Reaction, user: Union[discord.Member, discord.User]):
    message_data = {
        "action_id": 5,
        "user": str(user),
        "message": extract_message_data(reaction.message),
        "reaction": extract_reaction_data(reaction)
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=reaction.message.guild.id)


@client.event
async def on_reaction_remove(reaction: discord.Reaction, user: Union[discord.Member, discord.User]):
    message_data = {
        "action_id": 6,
        "user": str(user),
        "message": extract_message_data(reaction.message),
        "reaction": extract_reaction_data(reaction)
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=reaction.message.guild.id)


@client.event
async def on_reaction_clear(message: discord.Message, reactions: list[discord.Reaction]):
    message_data = {
        "action_id": 7,
        "message": extract_message_data(message),
        "reaction": [extract_reaction_data(reaction)for reaction in reactions]
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=message.guild.id)


@client.event
async def on_reaction_clear_emoji(reaction: discord.Reaction):
    message_data = {
        "action_id": 8,
        "message": extract_message_data(reaction.message),
        "reaction": extract_reaction_data(reaction)
    }
    send_message(producer=producer, message_data=message_data,
                 message_key=reaction.message.guild.id)

client.run(bot_token)

producer.flush()
