from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, count, year, to_date,min, max, avg, round


# create a SparkSession
spark = SparkSession.builder.appName("Earthquakes").getOrCreate()

# read the earthquakes.csv file as a Spark DataFrame
df = spark.read.csv("earthquakes.csv", header=True, inferSchema=True)

# need to convert the datetime column to proper date and time since there are inconsistencies (1/15/1965 vs 03/09/1965) so no easy way to pick the year
df = df.withColumn("DATE", to_date("DATETIME", "M/d/y H:m"))

'''
Q1
'''

# select the columns of interest
selected_cols = ["DATE", "MAGNITUDE"]

# sort the DataFrame by magnitude in descending order
sorted_df = df.select(selected_cols).orderBy(desc("MAGNITUDE"))

# take the first ten rows
top_10 = sorted_df.limit(10)

# show the results
top_10.show()



'''
Q2
'''
#create a Year column to store the year from the formated date
df_year = df.withColumn("YEAR", year("DATE"))


# filter the DataFrame to include only earthquakes after 2000 and group the number of earthquakes per year
earthquakes_2000 = df_year.filter(df_year["YEAR"] >= 2000)\
                          .groupBy("YEAR").agg(count("*").alias("NUM_EARTHQUAKES"),
                                               max("MAGNITUDE").alias("NUM_EARTHQUAKES")
                                               )\
                          .orderBy("YEAR")

# show the results
earthquakes_2000.show()



'''
Q3
'''

# Filter earthquakes between years 2010 and 2020
earthquakes_2010_2020 = df.filter((year("DATE") >= 2010) & (year("DATE") <= 2020))

# Group by year and calculate min, max, and avg magnitude
agg_df = earthquakes_2010_2020.groupBy(year("DATE").alias("YEAR")) \
        .agg(min("MAGNITUDE").alias("MIN_MAGNITUDE"), 
         max("MAGNITUDE").alias("MAX_MAGNITUDE"), 
         round(avg("MAGNITUDE"),2).alias("AVG_MAGNITUDE"))

# Show results
agg_df.show()



'''
Q4
'''

# Filter earthquakes in the Athens area based on lat long window
athens_df = df.filter((df["LAT"] >= 37.5) & (df["LAT"] <= 39.0) & (df["LONG"] >= 23.35) & (df["LONG"] <= 23.55))

# Sort by magnitude in descending order and select the top five
top_five_df = athens_df.sort(desc("MAGNITUDE")).limit(5)

# Show the results
top_five_df.show()
