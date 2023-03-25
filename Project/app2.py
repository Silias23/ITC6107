from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, to_date, avg, stddev
from pyspark.sql.functions import year, col, month, date_format
from pyspark.sql.functions import format_string
from pyspark.sql.types import DoubleType, StringType
import pandas as pd
# Create a SparkSession
spark = SparkSession.builder.appName("Investor Portfolios").getOrCreate()


portfolio_data = spark.read.csv("BGF-WTF-A2-EUR_fund1(1).csv", header=True, inferSchema=True)

portfolio_data = portfolio_data.withColumn("As Of", to_date("As Of", "d-MMM-yy"))


# Retrieve data from each portfolio table and store it in a dictionary
'''
Q1
'''

# daily_changes = portfolio_data.select((col("Daily NAV Change")).alias("Daily Change"))
# percentage_changes = portfolio_data.select((col("Daily NAV Change %")).alias("Percentage Change"))
# max_daily_change = daily_changes.select(col("Daily Change")).orderBy(col("Daily Change").desc()).first()[0]
# min_daily_change = daily_changes.select(col("Daily Change")).orderBy(col("Daily Change").asc()).first()[0]
# max_percentage_change = percentage_changes.select(col("Percentage Change")).orderBy(col("Percentage Change").desc()).first()[0]
# min_percentage_change = percentage_changes.select(col("Percentage Change")).orderBy(col("Percentage Change").asc()).first()[0]
# print(f"Maximum Daily Change: {max_daily_change:.2f}")
# print(f"Minimum Daily Change: {min_daily_change:.2f}")
# print(f"Maximum Percentage Change: {max_percentage_change:.2f}%")
# print(f"Minimum Percentage Change: {min_percentage_change:.2f}%")

def result_writer(file,description,filename):
    spark_df = file
    pandas_df = spark_df.toPandas()
    with open(f'{filename}.txt', 'a') as f:
        f.write(f'{description}')
        pandas_df.to_csv(f,sep='\t', index = False)
        f.write('\n')




def calculate_portfolio_statistics(df,filename):

    portfolio_stats = df.agg(max("Daily NAV Change").alias("Max(Daily NAV Change)"),
                                min("Daily NAV Change").alias("Min(Daily NAV Change)"),
                                max("Daily NAV Change %").alias("Max(Daily NAV Change %)"),
                                min("Daily NAV Change %").alias("Min(Daily NAV Change %)")
                                    )
    portfolio_stats.show()
    result_writer(portfolio_stats,'Portfolio Stats:\n',filename)
    return portfolio_stats



'''
Q2
'''



def portfolio_calc_year(df,filename):
    # Add a "Year" column to the DataFrame
    df_with_year = df.withColumn("Year", year(col("As Of")))
    
    # Group the DataFrame by year and calculate the maximum and minimum daily changes and percentage changes for each year
    stats_by_year = df_with_year.groupBy("Year")\
                                .agg(max("Daily NAV Change").alias("Max(Daily NAV Change)"),
                                    min("Daily NAV Change").alias("Min(Daily NAV Change)"),
                                    max("Daily NAV Change %").alias("Max(Daily NAV Change %)"),
                                    min("Daily NAV Change %").alias("Min(Daily NAV Change %)")
                                        )\
                                .orderBy("Year")                    
    
    stats_by_year.show()
    result_writer(stats_by_year,'Portfolio Stats Per Year:\n',filename)
    return stats_by_year






'''
Q3
'''


def calculate_portfolio_avg_std(df,filename):

    portfolio_avg_std = df.agg(avg("NAV per Share").alias("Average Evaluation"),
                                stddev("NAV per Share").alias("Standard Deviation"),
                            )
    
    portfolio_avg_std.show()
    result_writer(portfolio_avg_std,'Portfolio Average and Std. Deviation:\n',filename)
    return portfolio_avg_std







'''
Q4
'''


def calculate_portfolio_avg_std_year(df,fyears,filename):
    #column used for the grouping, the filtering will be made based on the conversion of 'As of' column
    df_with_year = df.withColumn("Year", year(col("As Of")))
    df_with_year = df_with_year.filter((year("As Of") >= fyears[0]) & (year("As Of") <= fyears[1]))

    portfolio_avg_std = df_with_year.groupBy("Year")\
                                    .agg(avg("NAV per Share").alias("Average Evaluation"),
                                        stddev("NAV per Share").alias("Standard Deviation"),
                                        )\
                                    .orderBy("Year")
    
    portfolio_avg_std.show()
    result_writer(portfolio_avg_std,f'Portfolio Average and Std. Deviation for years {fyears[0]} - {fyears[1]}:\n',filename)
    return portfolio_avg_std




'''
Q5
'''

def calculate_portfolio_month_avg(df,filename):

    # Add a "Month" column to the DataFrame
    df_with_month = df.withColumn("Month", date_format(to_date(col("As Of"), "yyyy-MM-dd"), 'yyyy-MM'))
    # df_with_month.show()
    # Group the DataFrame by month and calculate the average "NAV per Share" for each month
    monthly_averages = df_with_month.groupBy("Month")\
                                    .agg(avg("NAV per Share").alias("Average NAV per Share"))\
                                    .orderBy(col("Month").desc())
    
    monthly_averages.show()
    result_writer(monthly_averages,'Monthly Averages:\n',filename)
    return monthly_averages


#calculate_portfolio_month_avg(portfolio_data)













# # Connect to the MySQL database
# url = "jdbc:mysql://<hostname>:<port>/<database>"
# properties = {
#     "driver": "com.mysql.jdbc.Driver",
#     "user": "<username>",
#     "password": "<password>"
# }

# # Define the SQL query to retrieve the portfolios linked to an investor
# investor_name = "Inv1"
# query = f"""
#     SELECT p.Name
#     FROM Portfolios p
#     INNER JOIN Investors_Portfolios ip ON p.Id = ip.pid
#     INNER JOIN Investors i ON i.Id = ip.iid
#     WHERE i.Name = '{investor_name}'
# """

# # Load the data into a Spark DataFrame
# df = spark.read.jdbc(url=url, table=query, properties=properties)

# # Convert the DataFrame to a Python list
# portfolio_list = [row["Name"] for row in df.collect()]

# # Print the list
# print(portfolio_list)


# # Define a function to retrieve data from each portfolio table
# def retrieve_portfolio_data(investor,portfolio):
#     query = f"SELECT * FROM {investor}_{portfolio}"
#     df = spark.read.jdbc(url=url, table=query, properties=properties)
#     return df

results = []

# for portfolio_name in portfolio_list:
#     portfolio_data = retrieve_portfolio_data(investor_name, portfolio_name)

filename = 'results' #f'{investor}_{portfolio}.txt'
calculate_portfolio_statistics(portfolio_data,filename)


portfolio_calc_year(portfolio_data,filename)
calculate_portfolio_avg_std(portfolio_data,filename)
yearrange = ['2019','2020']
calculate_portfolio_avg_std_year(portfolio_data,yearrange,filename)
calculate_portfolio_month_avg(portfolio_data,filename)




