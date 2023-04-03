from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=msgpack.dumps,retries=5)

# Asynchronous by default
future = producer.send('my-topic', {'key': 'value'})

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    print("exception moment, idk waht lmao")
    pass

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)



# # produce asynchronously
# for _ in range(100):
#     producer.send('my-topic', b'msg')

# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)

# def on_send_error(excp):


#     print('I am an errback',excp)
#     # handle exception

# # produce asynchronously with callbacks
# producer.send('my-topic', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()