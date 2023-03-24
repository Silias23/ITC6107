from kafka import KafkaConsumer
from kafka import TopicPartition
from json import loads

kdszer = lambda x: x.decode('utf-8')
vdszer = lambda x: loads(x.decode('utf-8'))
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         group_id='g1',
                         key_deserializer=kdszer,
                         value_deserializer=vdszer)
consumer.assign([TopicPartition('t2p', 1)])

for msg in consumer:
    msgkey = msg.key
    msgval = msg.value
    print(f'Received key: {msgkey}, value {msgval}')

