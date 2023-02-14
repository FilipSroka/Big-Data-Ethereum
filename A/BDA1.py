from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                yield (datetime.datetime.fromtimestamp(int(fields[6])).strftime("%B %Y"), 1) # date, count
        except:
            pass

    def combiner(self, key, values):
        yield (key, sum(values)) # date, count

    def reducer(self, key, values):
        yield (key, sum(values)) # date, count


if __name__ == '__main__':
    BDA.run()
