import json

from kafka import KafkaConsumer
from json import loads
import mysql.connector
from decimal import Decimal

mydb = mysql.connector.connect(user='itc6107', password='itc6107', host='127.0.0.1',
                               database='InvestorsDB')
# Create cursor
mycursor = mydb.cursor()

# Create Kafka consumer
consumer = KafkaConsumer(
    'portfolios',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group1',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# Loop through Kafka messages and add data to MySQL database
for message in consumer:
    json_object1 = message.value
    json_object = json.loads(json_object1)
    print(json_object)
    # split JSON to each variable
    investor = json_object["investor"]
    portfolio_name = json_object["portfolio_name"]
    date = json_object["Date"]
    total_value = json_object["total_value"]
    daily_change = json_object["daily_change"]
    percentage_change = json_object["percentage_change"]
    # use the investor and portfolio name to build the table name to insert the data
    table_name = f"{investor}_{portfolio_name}"
    # insert data into table
    sql = f"INSERT INTO {table_name} (Date, total_value, daily_change, percentage_change) VALUES (%s, %s, %s, %s)"
    val = (date, str(total_value), str(daily_change), str(percentage_change))
    mycursor.execute(sql, val)
    mydb.commit()

# Close MySQL connection
mydb.close()
