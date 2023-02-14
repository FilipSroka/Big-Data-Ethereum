from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                yield (fields[2], int(fields[3])) # to_address, value
        except:
            pass

    def combiner(self, key, values):
        yield (key, sum(values)) # to_address, sum of values

    def reducer(self, key, values):
        yield (key, sum(values)) # to_address, total value


if __name__ == '__main__':
    BDA.run()
