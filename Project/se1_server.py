import json
from datetime import date, timedelta
import time
import holidays
import random
from kafka import KafkaProducer, TopicPartition
from kafka import KafkaAdminClient
from kafka.admin import NewPartitions, NewTopic


BROKER_URL = "localhost:9092"
TOPIC_NAME = ["StockExchange", "portfolios"]

stock1 = [
    ('IBM', 128.25), ('AAPL', 151.60), ('FB', 184.51), ('AMZN', 93.55),
    ('GOOG', 93.86), ('META', 184.51), ('MSI', 265.96), ('INTC', 25.53),
    ('AMD', 82.11), ('MSFT', 254.15), ('DELL', 38.00), ('ORKL', 88.36)
]


def delete_topics(topic_name):
    try:
        topic = [topic_name]
        admin_client.delete_topics(topics=topic)
        time.sleep(2)
        print(f'topic {topic_name} deleted successfully')
    except Exception as e:
        print(f'Error: \n{e}')


def create_topics(topic_name, partitions):
    try:
        topic_list = []
        topic = topic_name
        topic_list.append(NewTopic(name=topic, num_partitions=partitions, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        time.sleep(2)
        print(f'topic {topic} created successfully')
    except Exception as e:
        print(f'Error: \n{e}')


admin_client = KafkaAdminClient(bootstrap_servers = BROKER_URL, client_id = 'test')


delete_topics(TOPIC_NAME[0])
create_topics(TOPIC_NAME[0], 2)
delete_topics(TOPIC_NAME[1])
create_topics(TOPIC_NAME[1], 1)

# topic_list= []
# topic_list.append(NewTopic(name=TOPIC_NAME[0], num_partitions=2, replication_factor=1))
# topic_list.append(NewTopic(name=TOPIC_NAME[1], num_partitions=1, replication_factor=1))
#
# admin_client.create_topics(new_topics=topic_list, validate_only=False)
# time.sleep(2)
# print(f'Topics {topic_list} created successfully')


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
        for s in stock1:
            ticker, price = s
            r = random.random() / 10 - 0.05
            price *= 1 + r
            msg = {"TICK": ticker, "PRICE": round(price, 2), "TS": str(today)}
            stock_msgs.append(msg)
        json_msg = json.dumps({"tickers": stock_msgs})
        print(json_msg)
        key = str(today)
        partition = 0
        producer.send(TOPIC_NAME[0], value=json_msg, key=key, partition=partition)

    time.sleep(2)
    today += timedelta(days=1)



'''
[{"TICK": "AAPL", "PRICE": 123.54, "TS": "2000-01-03"}, {"TICK": "GOOG", "PRICE": 1648.45, "TS": "2000-01-03"}, {"TICK": "MSFT", "PRICE": 241.65, "TS": "2000-01-03"}, {"TICK": "TSLA", "PRICE": 679.77, "TS": "2000-01-03"}, {"TICK": "INTC", "PRICE": 64.34, "TS": "2000-01-03"}, {"TICK": "CSCO", "PRICE": 51.02, "TS": "2000-01-03"}, {"TICK": "AMZN", "PRICE": 2897.68, "TS": "2000-01-03"}, {"TICK": "FB", "PRICE": 278.03, "TS": "2000-01-03"}, {"TICK": "NVDA", "PRICE": 238.05, "TS": "2000-01-03"}, {"TICK": "PYPL", "PRICE": 255.25, "TS": "2000-01-03"}, {"TICK": "ADBE", "PRICE": 350.48, "TS": "2000-01-03"}, {"TICK": "NFLX", "PRICE": 540.44, "TS": "2000-01-03"}]
[{"TICK": "HPQ", "PRICE": 26.71, "TS": "2000-01-03"}, {"TICK": "AAPL", "PRICE": 68.61, "TS": "2000-01-03"}, {"TICK": "QCOM", "PRICE": 121.29, "TS": "2000-01-03"}, {"TICK": "VZ", "PRICE": 36.48, "TS": "2000-01-03"}, {"TICK": "TXN", "PRICE": 173.67, "TS": "2000-01-03"}, {"TICK": "CRM", "PRICE": 176.88, "TS": "2000-01-03"}, {"TICK": "AVGO", "PRICE": 622.23, "TS": "2000-01-03"}, {"TICK": "VMW", "PRICE": 119.82, "TS": "2000-01-03"}, {"TICK": "EBAY", "PRICE": 43.66, "TS": "2000-01-03"}, {"TICK": "IBM", "PRICE": 119.69, "TS": "2000-01-03"}, {"TICK": "ORCL", "PRICE": 67.31, "TS": "2000-01-03"}, {"TICK": "INTU", "PRICE": 412.94, "TS": "2000-01-03"}]
Examining date: 2000-01-03
Total portfolio value: 11482839.00, 0.0, 0.0
Examining date: 2000-01-03
Total portfolio value: 538036.00, 0.0, 0.0

'''
