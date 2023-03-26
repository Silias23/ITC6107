import json
from datetime import date, timedelta
import time
import holidays
import random
from kafka import KafkaProducer


BROKER_URL = "localhost:9092"
TOPIC_NAME = "StockExchange"

stock2 = [
    ('HPQ', 27.66), ('CSCO', 48.91), ('ZM', 69.65), ('QCOM', 119.19),
    ('ADBE', 344.80), ('VZ', 37.91), ('TXN', 172.06), ('CRM', 182.32),
    ('AVGO', 625.15), ('NVDA', 232.88), ('VMW', 120.05), ('EBAY', 43.98)
]

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
        for s in stock2:
            # for every stock calculate the new price
            ticker, price = s
            r = random.random() / 10 - 0.05
            price *= 1 + r
            msg = {"TICK": ticker, "PRICE": round(price, 2), "TS": str(today)}
            stock_msgs.append(msg)
        # create one json object that contains all ticker prices for the day.
        json_msg = json.dumps({"tickers": stock_msgs})
        print(json_msg)
        # always use the second partition to send the message
        key = str(today)
        partition = 1
        producer.send(TOPIC_NAME, value=json_msg, key=key, partition=partition)

    # simulate the day and change the date to the next one
    time.sleep(2)
    today += timedelta(days=1)
