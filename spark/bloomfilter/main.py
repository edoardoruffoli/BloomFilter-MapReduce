import numpy as np
from pyspark import SparkContext
from decimal import Decimal, ROUND_HALF_UP

def array_split(line):
    items = line.split("\t")
    #return items[0], int(round(float(items[1]) + 0.1, 0))
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))

sc = SparkContext(appName="Bloomfilter", master="local[*]")

rdd_file = sc.textFile("film-ratings.txt").map(array_split)
counts = rdd_file.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y)
print(counts.collect())

