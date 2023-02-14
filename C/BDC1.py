from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 9:
                yield (fields[2], int(fields[4])) # miner, size
        except:
            pass

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    BDA.run()
