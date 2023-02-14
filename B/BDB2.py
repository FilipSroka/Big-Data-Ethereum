from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 1: # trasaction
                fields = line.split("\t")
                if len(fields) == 2:
                    yield (fields[0][1:-1], ("T", int(fields[1]))) # to_adress, (indicator, total value)
            elif len(fields) == 5: # contract
                yield (fields[0], ("C")) # address, indicator
        except:
            pass

    def reducer(self, key, values):
        total = 0
        exists = False
        for i in values:
            if i[0] == "T":
                total += i[1]
            else:
                exists = True
        if total and exists:
            yield (key, total) # to_address, total value


if __name__ == '__main__':
    BDA.run()
