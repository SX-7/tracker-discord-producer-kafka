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


@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    message_data = {"action":"Message sent",
                    "action_id":1,
                    "author": message.author.name+"#"+message.author.discriminator,
                    "author_id":message.author.id,
                    "content": message.system_content,
                    "channel": message.channel.name,
                    "created":message.created_at.isoformat(' ',timespec='seconds'),
                    "url":message.jump_url,
                    "type":message.type,
                    "embeds":[embed.to_dict() for embed in message.embeds],
                    "attachments":[attachment.url for attachment in message.attachments]
                    }

    # Asynchronous by default
    future = producer.send(topic='my-topic', value=message_data,
                           key=message.author.name+"#"+message.author.discriminator)

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
        print("%s:%d:%d" % (record_metadata.topic,
                            record_metadata.partition, record_metadata.offset))
    except KafkaError as err:
        # Decide what to do if produce request failed...
        print(err.args)
        pass


client.run(bot_token)

producer.flush()
