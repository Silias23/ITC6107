from pyspark import SparkContext
import findspark

findspark.init()


sc = SparkContext("local[2]", "Airline Total Delay")
airline_rdd = sc.textFile("ontime_flights.csv")

rdd = airline_rdd.filter(lambda row: "Date" not in row )\
                     .map(lambda row: row.split(','))


'''
Q1
'''

# map each row to a tuple of (airline, totalDelay)
airline_delay_rdd = rdd.map(lambda row: (row[2], int(row[7]or 0) + int(row[10]or 0)))

# get the airline with maximum total delay
max_airline = airline_delay_rdd.max(key=lambda x: x[1])[0]

# print the max_airline
print(f"The airline with the maximum total (departure and arrival) delay is {max_airline}")



'''
Q2
'''
#map the airport and departure delay, filter if airport is JFK
deptDelaysJFK = rdd.map(lambda row: (row[3], int(row[7]or 0)))\
                   .filter(lambda x: x[0] == "JFK")\
                   .map(lambda x: x[1])

# Compute the average departure delay for flights departing from JFK
avgDeptDelayJFK = deptDelaysJFK.mean()

# Print the average departure delay for flights departing from JFK
print(f"Average departure delay for flights departing from JFK: {avgDeptDelayJFK:.2f}")



'''
Q3
'''
#map the days and total delay, reduce using the date as key to find the total delay for each day
TotalDelaysPerDay = rdd.map(lambda row: (row[0], int(row[7]or 0) + int(row[10]or 0)))\
                       .reduceByKey(lambda x,y: x+y)

for date, totalDelay in TotalDelaysPerDay.collect():
    print(f"{date}  total delay: {totalDelay}")




'''
Q4
'''

# keep the name and calculate the total delay, filter if the airline is AA and keep as a value the total delay
delaysAA = rdd.map(lambda row: (row[2], int(row[7]or 0) + int(row[10]or 0))).filter(lambda x: x[0] == "AA").map(lambda x: x[1])
avgTotalDelayAA = delaysAA.mean()

# Print the average total delay for American Airlines (AA)
print("Average total delay for American Airlines (AA): " + str(avgTotalDelayAA))




'''
Q5
'''

# map only the fields we need on a new rdd, date, airport from and to and filter the data to keep only the relevant records
flightsJFKtoLAX = rdd.map(lambda row: (row[0], row[3], row[4])).filter(lambda x: x[1] == "JFK" and x[2] == "LAX")

# Compute the number of flights 
numFlightsJFKtoLAX = flightsJFKtoLAX.countByKey()

# For each day print the number of flights
for date, numFlights in numFlightsJFKtoLAX.items():
    print(date + " flights from JFK to LAX: " + str(numFlights))