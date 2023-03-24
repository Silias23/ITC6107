from kafka import KafkaProducer
from time import sleep
from json import dumps

def partitioner(key):
    return int(key[3:]) % 2

kszer = lambda x: x.encode('utf-8')
vszer = lambda x: dumps(x).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         #partitioner=partitioner,
                         key_serializer=kszer,
                         value_serializer=vszer)

for e in range(1000):
    key = 'Key' + str(e)
    val = {'Num': e}
    partition = partitioner(key)
    producer.send('t2p', partition=partition, key=key, value=val)
    print(f'Produced key: {key}, value: {val} into partition {partition}.')
    sleep(5)
