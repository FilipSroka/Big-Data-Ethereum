from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split("\t")
            if len(fields) == 2:
                yield (fields[0][1:-1], int(fields[1][:-1])) # category, value
        except:
            pass

    def combiner(self, key, values):
        yield (key, sum(values)) # category, sum of values

    def reducer(self, key, values):
        yield (key, sum(values)) # category, total value

if __name__ == '__main__':
    BDA.run()
