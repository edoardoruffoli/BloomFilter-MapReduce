import math

import numpy as np
from pyspark import SparkContext
from decimal import Decimal, ROUND_HALF_UP
import bloomfilter
from bloomfilter.bloomfilter import Bloomfilter

p = 0.01


def array_split(line):
    items = line.split("\t")
    #return items[0], int(round(float(items[1]) + 0.1, 0))
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))


def init_bloomfilter(n, p):
    m = int(round((-n*math.log(p))/(math.log(2)**2)))
    k = int(round(m*math.log(2)/n))
    return m, k


def bloomfilter_population(lines):
    bloomfilters = [Bloomfilter(m, k) for m, k in broadcast_bf_param.value]
    for line in lines:
        (id, rate) = array_split(line)
        bloomfilters[rate-1].add(id)
    rate = range(1, 11)
    return zip(rate, bloomfilters)


def bloomfilter_validation(lines):
    counter = [0 for i in range(10)]
    for line in lines:
        (filmId, rate) = array_split(line)
        for bf in broadcast_bf.value:
            if rate != bf[0]:
                result = bf[1].find(filmId)
                if result is True:
                    counter[rate-1] = counter[rate-1] + 1

    return counter


if __name__ == "__main__":
    sc = SparkContext(appName="Bloomfilter", master="local[*]")

    rdd_file = sc.textFile("film-rating.txt").map(array_split)

    # creation
    counts = rdd_file.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).sortByKey()
    bloomfilters_param = [init_bloomfilter(n, p) for rating, n in counts.collect()]
    broadcast_bf_param = sc.broadcast(bloomfilters_param)
    print(broadcast_bf_param.value)

    # population
    rdd_chunk = sc.textFile("film-rating.txt").mapPartitions(bloomfilter_population)
    rdd_bloomfilter = rdd_chunk.reduceByKey(lambda filter1, filter2: filter1.bitwise_or(filter2))
    print(rdd_bloomfilter)

    # validation
    broadcast_bf = sc.broadcast(rdd_bloomfilter.collect())
    rdd_counter = sc.textFile("film-rating.txt").mapPartitions(bloomfilter_validation)
    print(rdd_counter)
    #rdd_p = rdd_counter.reduce(lambda counter1, counter2: [sum(x) for x in zip(counter1, counter2)])
   # print(rdd_p)



