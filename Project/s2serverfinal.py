import json
from datetime import date, timedelta
import time
import holidays
import random
from kafka import KafkaProducer

PORT = 9999
BROKER_URL = "localhost:9092"
TOPIC_NAME = "StockExchange"

stock2 = [
    ('HPQ', 27.66), ('ZM', 69.65), ('QCOM', 119.19), ('VZ', 37.91),
    ('TXN', 172.06), ('CRM', 182.32), ('AVGO', 625.15), ('VMW', 120.05),
    ('EBAY', 43.98), ('IBM', 121.34), ('ORCL', 64.91), ('INTU', 418.64)
]

producer = KafkaProducer(
    bootstrap_servers=BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gr_holidays = holidays.GR()

today = date(2000, 1, 1)

while True:
    if today.weekday() < 5 and today not in gr_holidays:
        stock_msgs = []
        for s in stock2:
            ticker, price = s
            r = random.random() / 10 - 0.05
            price *= 1 + r
            msg = {"TICK": ticker, "PRICE": round(price, 2), "TS": str(today)}
            stock_msgs.append(msg)
            producer.send(TOPIC_NAME, value=msg)
        json_msg = json.dumps({"tickers": stock_msgs})
        print(json_msg)

    time.sleep(2)
    today += timedelta(days=1)
