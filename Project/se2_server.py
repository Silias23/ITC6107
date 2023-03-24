import json
from datetime import date, timedelta
import time
import holidays
import random
from kafka import KafkaProducer, TopicPartition
from kafka import KafkaAdminClient
from kafka.admin import NewPartitions, NewTopic


BROKER_URL = "localhost:9092"
TOPIC_NAME = "StockExchange"

stock2 = [
    ('HPQ', 27.66), ('CSCO', 48.91), ('ZM', 69.65), ('QCOM', 119.19),
    ('ADBE', 344.80), ('VZ', 37.91), ('TXN', 172.06), ('CRM', 182.32),
    ('AVGO', 625.15), ('NVDA', 232.88), ('VMW', 120.05), ('EBAY', 43.98)
]

# admin_client = KafkaAdminClient(bootstrap_servers = BROKER_URL, client_id = 'test')
# topic_list= []
# topic_list.append(NewTopic(name=TOPIC_NAME, num_partitions=2, replication_factor=1))
#
# admin_client.create_topics(new_topics=topic_list,validate_only=False)


kszer = lambda x: x.encode('utf-8')
producer = KafkaProducer(
    bootstrap_servers=BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer = kszer
)

gr_holidays = holidays.GR()

today = date(2000, 1, 1)

while True:
    # print(producer.partitions_for(TOPIC_NAME))
    if today.weekday() < 5 and today not in gr_holidays:
        stock_msgs = []
        for s in stock2:
            ticker, price = s
            r = random.random() / 10 - 0.05
            price *= 1 + r
            msg = {"TICK": ticker, "PRICE": round(price, 2), "TS": str(today)}
            stock_msgs.append(msg)
        json_msg = json.dumps({"tickers": stock_msgs})
        print(json_msg)
        key = str(today)
        partition = 1
        producer.send(TOPIC_NAME, value=json_msg, key=key, partition=partition)

    time.sleep(2)
    today += timedelta(days=1)
