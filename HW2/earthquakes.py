from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
import heapq
from heapq import nlargest


# Class that finds the top 10 most powerful earthquakes in Greece
class Earthquakes_Top10(MRJob):
    
    # get the date, time, and magnitude of each earthquake
    def mapper_top10(self, _, line):    
        fields = line.split(",")
        date_str = fields[0].strip()
        date = datetime.strptime(date_str, "%m/%d/%Y %H:%M")
        mag = float(fields[4].strip())
        # Yield the magnitude and date/time for each earthquake
        yield None, (mag, date.strftime("%m/%d/%Y %H:%M"))
        
    # Reducer function that finds the top 10 most powerful earthquakes and outputs their date/time and magnitude
    def reducer_top10(self, _, datetimes_and_magnitudes):
        # Extract the top 10 datetimes with the highest magnitude
        top10_datetimes_and_magnitudes = nlargest(10, datetimes_and_magnitudes, key=lambda x: x[0])
        
        # Output each top 10 datetime along with its magnitude
        for datetime, magnitude in top10_datetimes_and_magnitudes:
            yield datetime, magnitude
            
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_top10, reducer=self.reducer_top10)
        ]
    
# Class that counts the number of earthquakes per year and month that struck Greece after 2010
class Earthquakes_Number(MRJob):

    #extract the year and month of each earthquake that occurred after 2010
    def mapper_2010(self, _, line):
        fields = line.split(",")
        date_str = fields[0].strip()
        date = datetime.strptime(date_str, "%m/%d/%Y %H:%M")
        # If the earthquake occurred after 2010, yield the year and month along with a count of 1
        if date.year >= 2010:
            year_month = date.strftime("%Y-%m")
            yield (year_month), 1
 

    #count the number of earthquakes per year and month
    def reducer_count(self, year_month, counts):

        yield year_month, sum(counts)
    

    def steps(self):
        return [
            MRStep(mapper=self.mapper_2010, reducer=self.reducer_count)
        ]
    

# Class that calculates the minimum, maximum, and average magnitude of earthquakes per year between 2010 and 2020
class Earthquakes_Stats(MRJob):

    #extract the year and magnitude of each earthquake that occurred between 2010 and 2020
    def mapper_stats(self, _, line):     
        fields = line.split(",")
        date_str = fields[0].strip()
        date = datetime.strptime(date_str, "%m/%d/%Y %H:%M")
        mag = float(fields[4].strip())       
        # If the earthquake occurred between 2010 and 2020, yield the year along with the magnitude, minimum magnitude, and a count of 1
        if 2010 <= date.year <= 2020:
            year = str(date.year)
            yield (year), (mag, mag, 1)
        

    def reducer_stats(self, year, mags):
        #initialize the min max counters
        min_mag = float("inf")
        max_mag = float("-inf")
        total_mag = 0
        count = 0
        #update the min max counters when necessary based on the new earthquakes
        for mag in mags:
            min_mag = min(min_mag, mag[0])
            max_mag = max(max_mag, mag[1])
            total_mag += mag[2]
            count += 1
        
        avg_mag = total_mag / count
        # Output the minimum, maximum, and average magnitude for the current year
        yield year, (min_mag, max_mag, avg_mag)
    
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_stats, reducer=self.reducer_stats)
        ]
    
# Class used to find the top 5 largest earthquakes that happened in Athens area
class Earthquakes_Athens(MRJob):
    #mapper to extract all fields from the file
    def mapper_athens(self, _, line):
        fields = line.split(",")
        date_str = fields[0].strip()
        date = datetime.strptime(date_str, "%m/%d/%Y %H:%M")
        lat = float(fields[1].strip())
        lon = float(fields[2].strip())
        mag = float(fields[4].strip())
        # If the location of the earthquake is inside the lon/lat of athens then yield the earthquake details
        if lat >= 37.5 and lat <= 39.0 and lon >= 23.35 and lon <= 23.55:
            yield None, (mag, fields)

    def reducer_athens(self, _, values):
        # Take the top 5 largest based on the magnitude and return them
        top_five = sorted(values, reverse=True)[:5]
        for mag, row in top_five:
            yield mag, row

    def steps(self):
        return [
            MRStep(mapper=self.mapper_athens, reducer=self.reducer_athens)
        ]
    
    


if __name__ == '__main__':
    Earthquakes_Top10.run()
    Earthquakes_Number.run()
    Earthquakes_Stats.run()
    Earthquakes_Athens.run()