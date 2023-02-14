from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7: # transaction
                yield (fields[2], (fields[6], fields[4])) # to_address, (date, gas)
            elif len(fields) == 5: # contract
                yield (fields[0], ("C",)) # address, (identifier)
        except:
            pass

    def reducer(self, key, values):
        l = []
        contract = False
        for i in values:
            if i[0] == "C":
                contract = True
            else:
                l.append(i)
        if contract:
            for i in l:
                yield (int(i[0]), int(i[1])) # date, gas

if __name__ == '__main__':
    BDA.run()
