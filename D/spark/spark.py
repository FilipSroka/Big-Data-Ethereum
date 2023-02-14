from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add
import time
import pyspark
import re

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7: # transactions
            str(fields[2])
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5: # contracts
            str(fields[0])
        else:
            return False
        return True
    except:
        return False

def main():
    times = []
    for i in range(5):
        t = time.time()
        partB()
        times.append(time.time() - t)
    print(times)

def partB():
    sc = pyspark.SparkContext()

    transactions = sc.textFile('hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions').filter(is_good_line) # validate lines
    contracts = sc.textFile('hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts').filter(is_good_line) # validated lines

    job1a = transactions.map(lambda line: (line.split(',')[2], int(line.split(',')[3]))) # splitting each line
    job1b = job1a.reduceByKey(lambda t1, t2: t1 + t2) # merge values for each key

    job2 = job1b.join(contracts.map(lambda line: (line.split(',')[0], 'C'))) # check if trasnaction in contracts

    job3 = job2.takeOrdered(10, key = lambda values: - values[1][0]) # get top 10

    for i in job3: # print top 10
        print(i[0], int(i[1][0]))

main()
