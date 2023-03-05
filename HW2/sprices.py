from mrjob.job import MRJob
import json

class StockPrices(MRJob):

    def mapper(self, _, line):
        # extract the names and price of each stock
        data = json.loads(line)
        yield data['TICK'], (float(data['PRICE']), 1)

    def reducer(self, key, values):
        max_price = float('-inf')
        min_price = float('inf')
        total_price = 0
        count = 0
        # calculate the stats for each stock
        for price, num in values:
            max_price = max(max_price, price)
            min_price = min(min_price, price)
            total_price += price
            count += num
        avg_price = total_price / count
        spread = (max_price - min_price) / avg_price * 100
        yield key, (round(max_price, 2), round(min_price, 2), round(avg_price, 2), round(spread, 2))

if __name__ == '__main__':
    StockPrices.run()
