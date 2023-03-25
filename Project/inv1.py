import json

from kafka import KafkaConsumer, KafkaProducer
from json import loads
from datetime import datetime
from kafka import TopicPartition

szer = lambda x: loads(x.decode('utf-8'))
kszer = lambda x: x.decode('utf-8')
pszer = lambda x: json.dumps(x).encode('utf-8')
TOPIC_NAME = "StockExchange"

consumer_0 = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                           auto_offset_reset='earliest', enable_auto_commit=True,
                           group_id='inv1', value_deserializer=szer,
                           key_deserializer=kszer)
consumer_0.assign([TopicPartition(TOPIC_NAME, 0)])


consumer_1 = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                           auto_offset_reset='earliest', enable_auto_commit=True,
                           group_id='inv1', value_deserializer=szer,
                           key_deserializer=kszer)
consumer_1.assign([TopicPartition(TOPIC_NAME, 1)])

# consumer_1 = KafkaConsumer('StockExchange', bootstrap_servers=['localhost:9092'],
#                            auto_offset_reset='earliest', enable_auto_commit=True,
#                            group_id='inv1', value_deserializer=szer)
# consumer_1.assign([TopicPartition('StockExchange', 1)])
  
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=pszer
)


def process_messages(messages, portfolio, previous_value,Iname,Pname):
    tickers = set(portfolio.keys())
    total_value = 0
    # print(f'message: \n{messages}')
    data = json.loads(messages)
    print(f'Examining date: {data["tickers"][0]["TS"]}')
    for ticker_info in data["tickers"]:
        # print(f'info: \n{ticker_info}')
        ticker = ticker_info['TICK']
        if ticker in tickers:
            price = float(ticker_info['PRICE'])
            quantity = portfolio[ticker]
            total_value += price * quantity
            # print(f"{ticker}: {quantity} shares, price: {price}, value: {price * quantity:.2f}")
    if previous_value == -9999999:
        previous_value = total_value
    daily_change =  total_value - previous_value
    percentage_change = daily_change/total_value*100
    previous_value = total_value
    print(f"Total portfolio value: {total_value:.2f}, {daily_change}, {percentage_change}")
    json_object = {
        "investor": Iname,
        "portfolio_name": Pname,
        "Date": data["tickers"][0]["TS"],
        "total_value": total_value,
        "daily_change": daily_change,
        "percentage_change": percentage_change
    }
    return previous_value, json_object


def sender(producer,message):
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

while True:

    message_partition_0 = next(consumer_0)
    message_value_partition_0 = message_partition_0.value
    # print(message_value_partition_0)
    message_partition_1 = next(consumer_1)
    message_value_partition_1 = message_partition_1.value

    dict1 = json.loads(message_value_partition_0)
    dict2 = json.loads(message_value_partition_1)


    merged = {"tickers": dict1['tickers'] + dict2['tickers']}
    day_messages = json.dumps(merged)

    portfolio_1_value,data1 = process_messages(day_messages, portfolio_1, portfolio_1_value,'Inv1','P11')
    sender(producer,data1)
    portfolio_2_value,data2 = process_messages(day_messages, portfolio_2, portfolio_2_value,'Inv1','P12')
    sender(producer,data2)

    
    # for msg in consumer_0:
    #     msgval = msg.value
    #     print(msgval)

'''
use if need to keep multiple messages in an array and process only hte first ones
'''
# # Start consuming messages from each partition
# while True:
#     # Read one message from partition 0
#     message_partition_0 = next(consumer_0)
#     message_value_partition_0 = message_partition_0.value
#     messages_buffer_partition_0.append(message_value_partition_0)
#     # Check if we have a message from both partitions
#     if len(messages_buffer_partition_0) > 0 and len(messages_buffer_partition_1) > 0:
#         # Process the two messages
#         process_messages([messages_buffer_partition_0[0], messages_buffer_partition_1[0]])
#         # Remove the processed messages from the buffers
#         messages_buffer_partition_0.pop(0)
#         messages_buffer_partition_1.pop(0)

#     # Read one message from partition 1
#     message_partition_1 = next(consumer_partition_1)
#     message_value_partition_1 = message_partition_1.value
#     messages_buffer_partition_1.append(message_value_partition_1)
#     # Check if we have a message from both partitions
#     if len(messages_buffer_partition_0) > 0 and len(messages_buffer_partition_1) > 0:
#         # Process the two messages
#         process_messages([messages_buffer_partition_0[0], messages_buffer_partition_1[0]])
#         # Remove the processed messages from the buffers
#         messages_buffer_partition_0.pop(0)
#         messages_buffer_partition_1.pop(0)