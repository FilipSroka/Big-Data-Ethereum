from mrjob.job import MRJob
import re
import datetime

class BDA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7: # transactions
                yield (fields[2], (datetime.datetime.fromtimestamp(int(fields[6])).strftime("%m %Y"), "T", int(fields[3]))) # to_address, (date, identifier, value)
            else: # scams
                for i in fields[3:]:
                    yield (i.strip(), (fields[1], "S")) # to_address, (category, identifier)
        except:
            pass

    def reducer(self, key, values):
        transactions, scams = False, False
        category = None
        l = []
        for i in values:
            if i[1] == "T":
                transactions = True
                l.append((i[0], i[2]))
            elif i[1] == "S":
                category = i[0]
                scams = True
        if transactions and scams:
            for i in l:
                yield (category, (i[0], i[1])) # category, (date, value)

if __name__ == '__main__':
    BDA.run()
