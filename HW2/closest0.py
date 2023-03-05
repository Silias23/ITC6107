from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq


class CloseToZero(MRJob):

    def mapper(self, _, line):
        #for every number in the file return a tuple with the value + the distance (abs) from 0
        for num in line.strip().split():
            num = float(num)
            yield None, (abs(num), num)

    def reducer_init(self):
        self.closest = []

    def reducer(self, key, nums):
        #push the numbers on a heap
        for num in nums:
            heapq.heappush(self.closest, num)

    def reducer_final(self):
        #take the 10 smallest numbers based on their absolute value and return the original number
        closest = heapq.nsmallest(10, self.closest, key=lambda x: x[0])
        for num in closest:
            yield  num[1], None

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    reducer_init=self.reducer_init,
                    reducer=self.reducer,
                    reducer_final=self.reducer_final)
        ]

if __name__ == '__main__':
    CloseToZero.run()
