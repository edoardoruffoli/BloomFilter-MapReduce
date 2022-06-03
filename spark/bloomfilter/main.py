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


def init_bloomfilter(n, p):
    m = int(round((-n*math.log(p))/(math.log(2)**2)))
    k = int(round(m*math.log(2)/n))
    return m, k


def bloomfilter_population(iterator):
    bloomfilters = [Bloomfilter(m, k) for m, k in broadcast_bf.value]
    return bloomfilters


sc = SparkContext(appName="Bloomfilter", master="local[*]")

rdd_file = sc.textFile("film-rating.txt").map(array_split)

# creation
counts = rdd_file.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y).sortByKey()
bloomfilters_param = [init_bloomfilter(n, p) for rating, n in counts.collect()]
broadcast_bf = sc.broadcast(bloomfilters_param)


# population
rdd_chunk = sc.textFile("film-rating.txt").mapPartitions(bloomfilter_population)

print(rdd_chunk.take(4))





