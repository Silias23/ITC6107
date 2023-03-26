from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, to_date, avg, stddev
from pyspark.sql.functions import year, col, date_format


# Select Investor to produce stats for their portfolios
investor_name = "Inv1"
# Select the year range used in Q4 for the portfolio evaluation
year_range = ['2010', '2012']


# function to write the results for each question in a text file
def result_writer(file, description, file_name):
    spark_df = file
    # convert to Pandas df to solve issues with datatypes
    # anything but stringtype cannot be written in file from a spark df
    pandas_df = spark_df.toPandas()
    with open(f'{file_name}_stats.txt', 'a') as f:
        f.write(f'{description}')
        pandas_df.to_csv(f, sep='\t', index=False)
        f.write('\n')


# given an investor name and portfolio, retrieve the table for processing
def retrieve_portfolio_data(investor, portfolio):
    query1 = f"(SELECT * FROM {investor}_{portfolio})as tmp"
    df_portfolio = spark_sql.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", query1) \
        .option("user", user) \
        .option("password", password).load()
    return df_portfolio


'''
Q1
'''
def calculate_portfolio_statistics(df, filename):
    # calculate the min and max for the daily and percentage change for the entire portfolio
    portfolio_stats = df.agg(max("daily_change").alias("Max(Daily Change)"),
                             min("daily_change").alias("Min(Daily Change)"),
                             max("percentage_change").alias("Max(Daily Change %)"),
                             min("percentage_change").alias("Min(Daily Change %)")
                             )
    portfolio_stats.show()
    result_writer(portfolio_stats, 'Portfolio Stats:\n', filename)
    return portfolio_stats


'''
Q2
'''
def portfolio_calc_year(df, filename):
    # Add a "Year" column to the DataFrame
    df_with_year = df.withColumn("Year", year(col("Date")))

    # Group the DataFrame by year and calculate
    # the maximum and minimum daily changes and percentage changes for each year
    stats_by_year = df_with_year.groupBy("Year") \
        .agg(max("daily_change").alias("Max(Daily Change)"),
             min("daily_change").alias("Min(Daily Change)"),
             max("percentage_change").alias("Max(Daily Change %)"),
             min("percentage_change").alias("Min(Daily Change %)")
             ) \
        .orderBy("Year")

    stats_by_year.show()
    result_writer(stats_by_year, 'Portfolio Stats Per Year:\n', filename)
    return stats_by_year


'''
Q3
'''
def calculate_portfolio_avg_std(df, filename):
    # calculate the average and standard deviation for the entire portfolio
    portfolio_avg_std = df.agg(avg("total_value").alias("Average Evaluation"),
                               stddev("total_value").alias("Standard Deviation"),
                               )
    portfolio_avg_std.show()
    result_writer(portfolio_avg_std, 'Portfolio Average and Std. Deviation:\n', filename)
    return portfolio_avg_std


'''
Q4
'''
def calculate_portfolio_avg_std_year(df, fyears, filename):
    # column used for the grouping, the filtering will be made based on the conversion of 'Date' column
    df_with_year = df.withColumn("Year", year(col("Date")))
    df_with_year = df_with_year.filter((year("Date") >= fyears[0]) & (year("Date") <= fyears[1]))

    portfolio_avg_std = df_with_year.groupBy("Year") \
        .agg(avg("total_value").alias("Average Evaluation"),
             stddev("total_value").alias("Standard Deviation"),
             ) \
        .orderBy("Year")

    portfolio_avg_std.show()
    result_writer(portfolio_avg_std, f'Portfolio Average and Std. Deviation for years {fyears[0]} - {fyears[1]}:\n',
                  filename)
    return portfolio_avg_std


'''
Q5
'''
def calculate_portfolio_month_avg(df, filename):
    # Add a "Month" column to the DataFrame
    df_with_month = df.withColumn("Month", date_format(to_date(col("Date"), "yyyy-MM-dd"), 'yyyy-MM'))
    # df_with_month.show()
    # Group the DataFrame by month and calculate the average "NAV per Share" for each month
    monthly_averages = df_with_month.groupBy("Month") \
        .agg(avg("total_value").alias("Average NAV per Share")) \
        .orderBy(col("Month").desc())

    monthly_averages.show()
    result_writer(monthly_averages, 'Monthly Averages:\n', filename)
    return monthly_averages


# parameters for the mysql db connection
url = "jdbc:mysql://localhost:3306/InvestorsDB"
driver = "com.mysql.jdbc.Driver"
user = "itc6107"
password = "itc6107"


# Use spark to connect to the sql db
spark_sql = SparkSession.builder.master("local[1]") \
    .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.28.jar") \
    .appName("6107app").getOrCreate()


# SQL query to retrieve the portfolios linked to an investor
query = f"(SELECT p.id \
        FROM Portfolios p \
        INNER JOIN Investors_Portfolios ip ON p.id = ip.pid \
        INNER JOIN Investors i ON i.id = ip.iid\
        WHERE i.id = '{investor_name}' \
        ) as tmp"

# extract the portfolio names linked to the investor
df = spark_sql.read.format("jdbc") \
    .option("url", url) \
    .option("driver", driver) \
    .option("dbtable", query) \
    .option("user", user) \
    .option("password", password).load()

# Convert the DataFrame to a Python list
portfolio_list = [row["id"] for row in df.collect()]


# for every portfolio name in the list, retrieve the data of the portfolio and produce all the stats
for portfolio_name in portfolio_list:
    portfolio_data = retrieve_portfolio_data(investor_name, portfolio_name)
    # portfolio_data.show()
    filename = f'{investor_name}_{portfolio_name}'  # f'{investor}_{portfolio}.txt'

    # Q1
    calculate_portfolio_statistics(portfolio_data, filename)
    # Q2
    portfolio_calc_year(portfolio_data, filename)
    # Q3
    calculate_portfolio_avg_std(portfolio_data, filename)
    # Q4
    calculate_portfolio_avg_std_year(portfolio_data, year_range, filename)
    # Q5
    calculate_portfolio_month_avg(portfolio_data, filename)