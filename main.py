from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
from dotenv import load_dotenv
import os
import discord

load_dotenv()

server_ip = os.getenv("SERVER_IP")
bot_token = os.getenv("DISCORD_BOT_TOKEN")

producer = KafkaProducer(bootstrap_servers=[str(server_ip+':9092')],
                         retries=5,
                         value_serializer=msgpack.dumps,
                         key_serializer=msgpack.dumps)

print(producer.bootstrap_connected())

intents = discord.Intents.all()

client = discord.Client(intents=intents)


def send_message(producer, message_key, message_data):
    future = producer.send(topic='my-topic', value=message_data,
                           key=message_key)
    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
        print("%s:%d:%d" % (record_metadata.topic,
                            record_metadata.partition, record_metadata.offset))
    except KafkaError as err:
        # Decide what to do if produce request failed...
        print(err.args)
        pass


@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    message_data = {
        "action": "Message sent",
        "action_id": 1,
        "author": message.author.name+"#"+message.author.discriminator,
        "author_id": message.author.id,
        "content": message.system_content,
        "channel": message.channel.name,
        "created": message.created_at.isoformat(' ', timespec='seconds'),
        "url": message.jump_url,
        "type": message.type,
        "embeds": [embed.to_dict() for embed in message.embeds],
        "attachments": [attachment.url for attachment in message.attachments]
    }
    send_message(producer=producer, message_data=message_data)

# TODO: Change to raws? To ensure catching
# TODO: passing keys?
# TODO: Make messages passed more compact
# TODO: increase consistency between messages

@client.event
async def on_message_edit(before, after):
    # action is consistent, we can have the same id too, author, channel, url is the same too?
    before_data = {
        "content": before.system_content,
        "created": before.created_at.isoformat(' ', timespec='seconds'),
        "type": before.type,
        "embeds": [embed.to_dict() for embed in before.embeds],
        "attachments": [attachment.url for attachment in before.attachments]
    }
    after_data = {
        "content": after.system_content,
        "edited": after.edited_at.isoformat(' ', timespec='seconds'),
        "type": after.type,
        "embeds": [embed.to_dict() for embed in after.embeds],
        "attachments": [attachment.url for attachment in after.attachments]
    }
    message_data = {
        "action": "Message edited",
        "action_id": 2,
        "author": after.author.name+"#"+after.author.discriminator,
        "author_id": after.author.id,
        "channel": after.channel.name,
        "url": after.jump_url,
        "before": before_data,
        "after": after_data
    }
    send_message(producer=producer, message_data=message_data)


@client.event
async def on_message_delete(message):
    message_data = {
        "action": "Message deleted",
        "action_id": 3,
        "author": message.author.name+"#"+message.author.discriminator,
        "author_id": message.author.id,
        "content": message.system_content,
        "channel": message.channel.name,
        "created": message.created_at.isoformat(' ', timespec='seconds'),
        "url": message.jump_url,
        "type": message.type,
        "embeds": [embed.to_dict() for embed in message.embeds],
        "attachments": [attachment.url for attachment in message.attachments]
    }
    send_message(producer=producer, message_data=message_data)


@client.event
async def on_bulk_message_delete(messages):
    message_data = {
        "action": "Messages bulk deleted",
        "action_id": 4,
        "messages": [
            {
                "author": message.author.name+"#"+message.author.discriminator,
                "author_id": message.author.id,
                "content": message.system_content,
                "channel": message.channel.name,
                "created": message.created_at.isoformat(' ', timespec='seconds'),
                "url": message.jump_url,
                "type": message.type,
                "embeds": [embed.to_dict() for embed in message.embeds],
                "attachments": [attachment.url for attachment in message.attachments]
            } for message in messages
        ]
    }
    send_message(producer=producer, message_data=message_data)


@client.event
async def on_reaction_add(reaction, user):
    message_data = {
        "action": "Reaction added",
        "action_id": 5,
        "reactee": user.name+"#"+user.discriminator,
        "message": {
            # TODO trim it down
            "action": "Message sent",
            "action_id": 1,
            "author": reaction.message.author.name+"#"+reaction.message.author.discriminator,
            "author_id": reaction.message.author.id,
            "content": reaction.message.system_content,
            "channel": reaction.message.channel.name,
            "created": reaction.message.created_at.isoformat(' ', timespec='seconds'),
            "url": reaction.message.jump_url,
            "type": reaction.message.type,
            "embeds": [embed.to_dict() for embed in reaction.message.embeds],
            "attachments": [attachment.url for attachment in reaction.message.attachments]
        },
        "reaction": reaction.emoji.name
    }
    send_message(producer=producer, message_data=message_data)


@client.event
async def on_reaction_remove(reaction, user):
    message_data = {
        "action": "Reaction removed",
        "action_id": 6,
        "reactee": user.name+"#"+user.discriminator,
        "message": {
            # TODO trim it down
            "action": "Message sent",
            "action_id": 1,
            "author": reaction.message.author.name+"#"+reaction.message.author.discriminator,
            "author_id": reaction.message.author.id,
            "content": reaction.message.system_content,
            "channel": reaction.message.channel.name,
            "created": reaction.message.created_at.isoformat(' ', timespec='seconds'),
            "url": reaction.message.jump_url,
            "type": reaction.message.type,
            "embeds": [embed.to_dict() for embed in reaction.message.embeds],
            "attachments": [attachment.url for attachment in reaction.message.attachments]
        },
        "reaction": reaction.emoji.name
    }
    send_message(producer=producer, message_data=message_data)


@client.event
async def on_reaction_clear(message, reactions):
    message_data = {
        "action": "Reaction bulk cleared",
        "action_id": 7,
        "message": {
            # TODO trim it down
            "action": "Message sent",
            "action_id": 1,
            "author": message.author.name+"#"+message.author.discriminator,
            "author_id": message.author.id,
            "content": message.system_content,
            "channel": message.channel.name,
            "created": message.created_at.isoformat(' ', timespec='seconds'),
            "url": message.jump_url,
            "type": message.type,
            "embeds": [embed.to_dict() for embed in message.embeds],
            "attachments": [attachment.url for attachment in message.attachments]
        },
        "reaction": [[reaction.emoji.name, reaction.emoji.count]for reaction in reactions]
    }
    send_message(producer=producer, message_data=message_data)


@client.event
async def on_reaction_clear_emoji(reaction):
    message_data = {
        "action": "Reaction fullly cleared",
        "action_id": 8,
        "message": {
            # TODO trim it down
            "action": "Message sent",
            "action_id": 1,
            "author": reaction.message.author.name+"#"+reaction.message.author.discriminator,
            "author_id": reaction.message.author.id,
            "content": reaction.message.system_content,
            "channel": reaction.message.channel.name,
            "created": reaction.message.created_at.isoformat(' ', timespec='seconds'),
            "url": reaction.message.jump_url,
            "type": reaction.message.type,
            "embeds": [embed.to_dict() for embed in reaction.message.embeds],
            "attachments": [attachment.url for attachment in reaction.message.attachments]
        },
        "reaction": {
            "name":reaction.emoji.name, 
            "count":reaction.emoji.count
        }
    }
    send_message(producer=producer, message_data=message_data)

client.run(bot_token)

producer.flush()
