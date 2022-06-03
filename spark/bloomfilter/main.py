import math

import numpy as np
from pyspark import SparkContext
from decimal import Decimal, ROUND_HALF_UP

from bloomfilter import Bloomfilter

p = 0.01


def array_split(line):
    items = line.split("\t")
    #return items[0], int(round(float(items[1]) + 0.1, 0))
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))


def bloomfilter_parmeters(n, p):
    m = int(round((-n*math.log(p))/(math.log(2)**2)))
    k = int(round(m*math.log(2)/n))
    return m, k


sc = SparkContext(appName="Bloomfilter", master="local[*]")

rdd_file = sc.textFile("film-raiting.txt").map(array_split)
counts = rdd_file.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y)
print(counts.collect())


bloomfilters = [Bloomfilter(bloomfilter_parmeters(n, p)) for n in counts]


