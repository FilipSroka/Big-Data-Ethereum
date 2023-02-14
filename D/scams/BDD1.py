from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7: # transactions
                yield (fields[2], (int(fields[3]), "T")) # to_address, (value, indicator)
            else: # scams
                for i in fields[3:]:
                    yield (i.strip(), (fields[1], "S")) # address, (category, idicator)
        except:
            pass

    def reducer(self, key, values):
        total = 0
        transactions, scams = False, False
        category = None
        for i in values:
            if i[1] == "T":
                total += i[0]
                transactions = True
            elif i[1] == "S":
                category = i[0]
                scams = True
        if transactions and scams:
            yield (category, total) # category, total value

if __name__ == '__main__':
    BDA.run()
