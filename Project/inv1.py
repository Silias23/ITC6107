import json
from kafka import KafkaConsumer, KafkaProducer
from json import loads
from kafka import TopicPartition


szer = lambda x: loads(x.decode('utf-8'))
kszer = lambda x: x.decode('utf-8')
pszer = lambda x: json.dumps(x).encode('utf-8')
TOPIC_NAME = "StockExchange"
group_id = 'Inv1'

# Create two consumers to read the StockExchange topic
consumer_0 = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                           auto_offset_reset='earliest', enable_auto_commit=True,
                           group_id=group_id, value_deserializer=szer,
                           key_deserializer=kszer)
consumer_0.assign([TopicPartition(TOPIC_NAME, 0)])

consumer_1 = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                           auto_offset_reset='earliest', enable_auto_commit=True,
                           group_id=group_id, value_deserializer=szer,
                           key_deserializer=kszer)
consumer_1.assign([TopicPartition(TOPIC_NAME, 1)])

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=pszer
)


def process_messages(messages, portfolio, previous_value, iname, pname):
    # receives the day's tickers, portfolio and previous portfolio evaluation
    # produces JSON object to be sent to Kafka topic
    tickers = set(portfolio.keys())
    total_value = 0
    # print(f'message: \n{messages}')
    data = json.loads(messages)
    print(f'Examining date: {data["tickers"][0]["TS"]}')
    # for the entire ticker list check if the ticker is part of the examined portfolio
    # if yes then add the value of the stock to the total value of the portfolio based on the bought quantity
    for ticker_info in data["tickers"]:
        # print(f'info: \n{ticker_info}')
        ticker = ticker_info['TICK']
        if ticker in tickers:
            price = float(ticker_info['PRICE'])
            quantity = portfolio[ticker]
            total_value += price * quantity
            # print(f"{ticker}: {quantity} shares, price: {price}, value: {price * quantity:.2f}")
    # if this is the first time processing the portfolio - it does not have a value from the previous day
    # then assign it the value of today
    if previous_value == -9999999:
        previous_value = total_value
    # calculate the required stats
    daily_change = total_value - previous_value
    percentage_change = daily_change / total_value * 100
    previous_value = total_value
    print(f"Total portfolio value: {total_value:.2f}, {daily_change}, {percentage_change}")
    # create the JSON object to be sent to the portfolios topic
    json_object = {
        "investor": iname,
        "portfolio_name": pname,
        "Date": data["tickers"][0]["TS"],
        "total_value": total_value,
        "daily_change": daily_change,
        "percentage_change": percentage_change
    }
    return previous_value, json_object


def sender(producer, message):
    # responsible for sending the portfolio evaluation to Kafka Topic
    topic = 'portfolios'
    json_msg = json.dumps(message)
    producer.send(topic=topic, value=json_msg)
    # print(json_msg)


portfolio_1 = {
    "IBM": 1300,
    "AAPL": 2200,
    "FB": 1900,
    "AMZN": 2500,
    "GOOG": 1900,
    "AVGO": 2400,
}

portfolio_2 = {
    "VZ": 2900,
    "INTC": 2600,
    "AMD": 2100,
    "MSFT": 1200,
    "DELL": 2700,
    "ORCL": 1200,
}

portfolio_1_value = -9999999
portfolio_2_value = -9999999
investor = 'Inv1'


while True:
    # each consumer will read its partition for the message from the servers and wait until the message is sent
    message_partition_0 = next(consumer_0)
    message_value_partition_0 = message_partition_0.value
    # print(message_value_partition_0)
    message_partition_1 = next(consumer_1)
    message_value_partition_1 = message_partition_1.value

    # take both messages from the partitions and combine them in one message
    # that corresponds to all ticker prices for the day
    dict1 = json.loads(message_value_partition_0)
    dict2 = json.loads(message_value_partition_1)
    merged = {"tickers": dict1['tickers'] + dict2['tickers']}
    day_messages = json.dumps(merged)

    # evaluate each portfolio and send the evaluation to the Portfolios topic
    portfolio_1_value, data1 = process_messages(day_messages, portfolio_1, portfolio_1_value, investor, 'P11')
    sender(producer, data1)
    portfolio_2_value, data2 = process_messages(day_messages, portfolio_2, portfolio_2_value, investor, 'P12')
    sender(producer, data2)
