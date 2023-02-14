from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                yield (datetime.datetime.fromtimestamp(int(fields[6])).strftime("%B %Y"), int(fields[3])) # date, value
        except:
            pass

    def combiner(self, key, values):
        total, n = 0, 0
        for i in values:
            total += i
            n += 1
        yield (key, (total, n)) # date, (sum of values, number of values)

    def reducer(self, key, values):
        total, n = 0, 0
        for i in values:
            total += i[0]
            n += i[1]
        yield (key, total/n) # date, average


if __name__ == '__main__':
    BDA.run()
