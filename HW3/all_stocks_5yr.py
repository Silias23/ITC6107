from pyspark.sql import SparkSession
from pyspark.sql.functions import col,year,month, max, avg


# create a spark session
spark = SparkSession.builder.appName("highest_volume_dates").getOrCreate()

# read the csv file into a dataframe
df = spark.read.csv("all_stocks_5yr.csv", header=True, inferSchema=True)

# df.show()


'''
Q1
'''

# group by "Name" and "date", and compute the maximum volume each ticker has per day
max_volume_df = df.groupBy("Name", "date").agg(max("volume").alias("max_volume")).orderBy('Name')

# For each ticker keep only the day with the largest volume of trades
max_volume_per_ticker = max_volume_df.groupBy("Name").agg(max("max_volume").alias("max_volume")).orderBy('Name')

# join the original DataFrame with the result of the previous step to get the date with the highest volume
result_df = df.join(max_volume_per_ticker, ["Name"]) \
              .where(col("volume") == col("max_volume")) \
              .select("Name", "date", "max_volume")

result_df.show()


'''
Q2
'''
#This assumes that "The average volume of transactions per month" groups all the months of all years. -- so only 12 rows will be output

# group the DataFrame by month and compute the average volume for each group
avg_volume_df = df.groupBy(month("date").alias("month")).agg(avg("volume").alias("avg_volume")).orderBy("month")

# display the results
avg_volume_df.show()



'''
Q3
'''

# group the DataFrame by ticker and compute the average volume for each group
avg_volume_df = df.groupBy("Name").agg(avg("volume").alias("avg_volume")).orderBy("Name")

# display the results
avg_volume_df.show()




'''
Q4
'''

# select the necessary columns
spread_df = df.select("date", "Name", (col("high") - col("low")).alias("spread"))

# group by Name and find the max spread for each ticker
max_spread = spread_df.groupBy("Name").agg({"spread": "max"}).withColumnRenamed("max(spread)", "max_spread")

# join the original DataFrame with the max spread DataFrame to get the dates with the highest spread per ticker
spread_result = spread_df.alias("a").join(max_spread.alias("b"), (col("a.Name") == col("b.Name")) & (col("a.spread") == col("b.max_spread")))


# display the result
spread_result.select("date", "a.Name", "spread").show()



'''
Q5
'''


# select only the rows with year 2016
df_2016 = df.filter(year("date") == 2016)

# group by Name and calculate the average close price
avg_close = df_2016.groupBy("Name")\
                   .avg("close")\
                   .withColumnRenamed("avg(close)", "avg_close")

# display the result
avg_close.show()



'''
Q6
'''

#select the distinct tickers
tickers = df.select("Name").distinct()

# display the result
tickers.show()
