import json
from datetime import date, timedelta
import time
import holidays
import random
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic


BROKER_URL = "localhost:9092"
TOPIC_NAME = ["StockExchange", "portfolios"]

stock1 = [
    ('IBM', 128.25), ('AAPL', 151.60), ('FB', 184.51), ('AMZN', 93.55),
    ('GOOG', 93.86), ('META', 184.51), ('MSI', 265.96), ('INTC', 25.53),
    ('AMD', 82.11), ('MSFT', 254.15), ('DELL', 38.00), ('ORKL', 88.36)
]


def delete_topics(topic_name):
    # delete the topic if it exists else show an error
    try:
        topic = [topic_name]
        admin_client.delete_topics(topics=topic)
        # wait to completely remove the topic
        time.sleep(2)
        print(f'topic {topic_name} deleted successfully')
    except Exception as e:
        print(f'Error: \n{e}')


def create_topics(topic_name, partitions):
    # create new topics
    try:
        topic_list = []
        topic = topic_name
        topic_list.append(NewTopic(name=topic, num_partitions=partitions, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        time.sleep(2)
        print(f'topic {topic} created successfully')
    except Exception as e:
        print(f'Error: \n{e}')


# create a kafka admin to handle topic creation and deletion from inside the code.
admin_client = KafkaAdminClient(bootstrap_servers=BROKER_URL, client_id='test')
delete_topics(TOPIC_NAME[0])
create_topics(TOPIC_NAME[0], 2)
delete_topics(TOPIC_NAME[1])
create_topics(TOPIC_NAME[1], 1)


kszer = lambda x: x.encode('utf-8')
producer = KafkaProducer(
    bootstrap_servers=BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=kszer
)

gr_holidays = holidays.GR()

today = date(2000, 1, 1)

while True:
    # print(producer.partitions_for(TOPIC_NAME))
    # if it is not a holiday, and it is a weekday then ticker prices need to be produced
    if today.weekday() < 5 and today not in gr_holidays:
        stock_msgs = []
        for s in stock1:
            # for every stock calculate the new price
            ticker, price = s
            r = random.random() / 10 - 0.05
            price *= 1 + r
            msg = {"TICK": ticker, "PRICE": round(price, 2), "TS": str(today)}
            stock_msgs.append(msg)
        # create one json object that contains all ticker prices for the day.
        json_msg = json.dumps({"tickers": stock_msgs})
        print(json_msg)
        # always use the first partition to send the message
        key = str(today)
        partition = 0
        producer.send(TOPIC_NAME[0], value=json_msg, key=key, partition=partition)

    # simulate the day and change the date to the next one
    time.sleep(2)
    today += timedelta(days=1)
