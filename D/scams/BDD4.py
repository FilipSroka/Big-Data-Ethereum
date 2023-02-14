from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields1 = line.split("\t")
            if len(fields1) == 2:
                fields2 = fields1[1].split(",")
                yield (fields1[0][1:-1] + " " + fields2[0][2:-1], int(fields2[1][:-2])) # category + date, value
        except:
            pass

    def combiner(self, key, values):
        yield (key, sum(values))

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    BDA.run()
