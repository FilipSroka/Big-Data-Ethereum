from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split("\t")
            if len(fields) == 2:
                yield (None, (fields[0][1:-1], int(fields[1]))) # None, (to_address, total value)
        except:
            pass

    def reducer(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup: tup[1])
        for i in range(10):
            yield (sorted_values[i][0], sorted_values[i][1])

if __name__ == '__main__':
    BDA.run()
