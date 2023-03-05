from mrjob.job import MRJob

class HighLowTen(MRJob):

    def mapper(self, _, line):
        #map the values and yield each
        for value in line.strip().split():
            yield "key", int(value)
    
    def reducer(self, key, values):
        low = []
        high = []
        #function to find the top ten highest and lowest values
        for value in values:
            if len(high) < 10: 
                high.append(value)
            else:
                min_high = min(high)
                if value > min_high:
                    high.remove(min_high)
                    high.append(value)
            if len(low) < 10:
                low.append(value)
            else:
                max_low = max(low)
                if value < max_low:
                    low.remove(max_low)
                    low.append(value)
        #sort the two lists
        low = sorted(low) 
        high = sorted(high, reverse=True)
        yield 'Lowest:', low
        yield 'Highest:', high

if __name__ == '__main__':
    HighLowTen.run()
