from pyspark import SparkContext
import findspark

findspark.init()

sc = SparkContext("local[1]", 'Fund Analysis')
rdd = sc.textFile('BGF-WTF-A2-EUR_fund1(1).csv')
header = rdd.first()
rdd = rdd.filter(lambda x: x != header)


rdd = rdd.map(lambda x: x.split(','))

# Convert the values to the appropriate datatype
rdd = rdd.map(lambda x: (x[0], float(x[1]), float(x[2]), float(x[3])))

'''
Q1
'''

# Calculate the maximum and minimum daily change and percentage change
max_daily_change = rdd.map(lambda x: x[2]).max()
min_daily_change = rdd.map(lambda x: x[2]).min()
max_percent_change = rdd.map(lambda x: x[3]).max()
min_percent_change = rdd.map(lambda x: x[3]).min()

print(f"Maximum Daily Change: {max_daily_change}")
print(f"Minimum Daily Change: {min_daily_change}")
print(f"Maximum Percent Change: {max_percent_change}%")
print(f"Minimum Percent Change: {min_percent_change}%")



'''
Q2
'''
# Calculate the sum and count of the NAV values
nav_sum_count = rdd.map(lambda x: (x[1], 1)).reduce(lambda x, y: (x[0]+y[0], x[1]+y[1]))

# Calculate the average NAV
nav_avg = nav_sum_count[0] / nav_sum_count[1]

# Calculate the standard deviation of NAV
nav_stddev = rdd.map(lambda x: x[1]).stdev()

print(f'Average NAV: {nav_avg:.2f}')
print(f'NAV Standard Deviation: {nav_stddev:.2f}')



'''
Q3
'''
#keep the years that end with '20' meaning 2020 and calculate the standard deviation
year_2020_rdd = rdd.filter(lambda x: x[0].endswith("20")).map(lambda x: float(x[1])).stdev()
print(f'NAV Standard Deviation for 2020: {year_2020_rdd:.2f}')




'''
Uncomment the following if Q4 expected to provide the Average NAV per month - group by month, disregarding the years (12 outputs)
'''
# def get_month(datestring):
#     return datestring.split('-')[1]

# rdd_with_month = rdd.map(lambda x: (get_month(x[0]), x[1]))
    
# # Use reduceByKey to get the count and total NAVs for each month
# monthly_stats = rdd_with_month.groupByKey().mapValues(lambda values: (
#         len(values), sum(values)))

# # Calculate the average NAV per month
# avg_per_month = monthly_stats.mapValues(lambda x: x[1] / x[0])

# # Sort the resulting RDD by month in descending order
# avg_per_month = avg_per_month.sortBy(lambda x: x[0], ascending=False)

# # Display the resulting RDD
# for month, avg_nav in avg_per_month.collect():
#     print(f"{month}: {avg_nav}")




'''
Q4
'''

from datetime import datetime

### find the average NAV per month per year
nav_rdd = rdd.map(lambda x: (datetime.strptime(x[0], '%d-%b-%y').strftime('%Y-%m'), x[1])) \
            .groupByKey() \
            .mapValues(lambda x: sum(x)/len(x)) \
            .sortByKey(ascending=False)

print("The average NAV per month per year is:")
for key,value in nav_rdd.collect():
    print(f"{key}: {value:.2f}")


