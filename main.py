from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
from dotenv import load_dotenv
import os

load_dotenv()

server_ip = os.getenv("SERVER_IP")

producer = KafkaProducer(bootstrap_servers=[str(server_ip+':9092')],
                         retries=5,
                         value_serializer=msgpack.dumps)

print(producer.bootstrap_connected())
# Asynchronous by default
future = producer.send(topic='my-topic', value='value')

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
    print("%s:%d:%d"%(record_metadata.topic,record_metadata.partition,record_metadata.offset))
except KafkaError as err:
    # Decide what to do if produce request failed...
    print(err.args)
    pass

producer.flush()
