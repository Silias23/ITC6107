from kafka import KafkaConsumer
from json import loads
from datetime import datetime
from kafka import TopicPartition

szer = lambda x: loads(x.decode('utf-8'))

consumer_0 = KafkaConsumer('StockExchange', bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         group_id='inv1', value_deserializer=szer)
consumer_0.assign([TopicPartition('StockExchange', 0)])



consumer_1 = KafkaConsumer('StockExchange', bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         group_id='inv1', value_deserializer=szer)
consumer_1.assign([TopicPartition('StockExchange', 1)])


# # Define function to process two messages at a time
# def process_messages(messages):
#     # Extract data from each message
#     tick1 = messages[0]['TICK']
#     price1 = float(messages[0]['PRICE'])
#     ts_str1 = messages[0]['TS']
#     ts1 = datetime.strptime(ts_str1, '%d/%m/%Y')

#     tick2 = messages[1]['TICK']
#     price2 = float(messages[1]['PRICE'])
#     ts_str2 = messages[1]['TS']
#     ts2 = datetime.strptime(ts_str2, '%d/%m/%Y')
#     # Perform calculations with the data
#     print(f"Processing messages: tick1={tick1}, price1={price1}, ts1={ts1}, tick2={tick2}, price2={price2}, ts2={ts2}")
#     # Add your calculations here

# def process_messages(messages):
#     portf1, porft2 = (0,0)
#     merged_tickers = []
#     for msg in messages:
#         # msg_data = json.loads(msg)
#         tickers = msg["tickers"]
#         for ticker in tickers:
#             merged_tickers.append(ticker)
#     for ticker in merged_tickers:
#         print(f'Processed row:{ticker}')
#     return portf1, porft2    


def process_messages(messages, portfolio):
    tickers = set(portfolio.keys())
    total_value = 0
    for message in messages:
        for ticker_info in message:
            ticker = ticker_info['TICK']
            if ticker in tickers:
                price = float(ticker_info['PRICE'])
                quantity = portfolio[ticker]
                total_value += price * quantity
                print(f"{ticker}: {quantity} shares, price: {price}, value: {price*quantity:.2f}")
    print(f"Total portfolio value: {total_value:.2f}")



portfolio_1 = {"IBM": 1300, 
               "AAPL": 2200,
               "FB": 1900,
               "AMZN": 2500,
               "GOOG": 1900,
               "TWTR": 2400,
               }

while True:
    
    message_partition_0 = next(consumer_0)
    message_value_partition_0 = message_partition_0.value


    message_partition_1 = next(consumer_1)
    message_value_partition_1 = message_partition_1.value


    day_messages = (message_value_partition_0, message_value_partition_1)
    process_messages(day_messages,portfolio_1)



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