from kafka import KafkaConsumer
from json import loads


szer = lambda x: loads(x.decode('utf-8'))

consumer_0 = KafkaConsumer('portfolios', bootstrap_servers=['localhost:9092'],
                           auto_offset_reset='earliest', enable_auto_commit=True,
                           group_id='port', value_deserializer=szer)



while True:
    message_partition_0 = next(consumer_0)
    message_value_partition_0 = message_partition_0.value
    print(message_value_partition_0)


