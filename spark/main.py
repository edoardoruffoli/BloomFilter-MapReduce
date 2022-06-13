import math
import sys
import time

from pyspark import SparkContext
from decimal import Decimal, ROUND_HALF_UP
from bloomfilter.bloomfilter import Bloomfilter
import configparser


def id_rate_split(line):
    items = line.split("\t")
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))


def init_bloomfilter(n, p):
    m = int(round((-n * math.log(p)) / (math.log(2) ** 2)))
    k = int(round(m * math.log(2) / n))
    return m, k


def insert_in_bloomfilters(lines):
    # setup
    bloomfilters = [Bloomfilter(m, k) for m, k in broadcast_bf_params.value]

    # computation
    for line in lines:
        (id, rate) = id_rate_split(line)
        bloomfilters[rate - 1].add(id)

    rate = range(1, 11)
    return zip(rate, bloomfilters)


def validate_bloomfilter(line):
    counter = []
    # for line in lines:
    (film_id, rate) = id_rate_split(line)
    for bf in broadcast_bf.value:
        if rate != bf[0]:
            result = bf[1].find(film_id)
            if result is True:
                counter.append(tuple((bf[0], 1)))
    return counter


if __name__ == "__main__":
    start_time = time.time()

    config = configparser.ConfigParser()
    config.read('config.properties')

    INPUT_PATH = config.get('Dataset', 'inputPath')
    OUTPUT_PATH = config.get('Dataset', 'outputPath')

    p = float(config.get('Bloom Filter', 'p'))

    sc = SparkContext(appName="BloomFilter", master="yarn")
    sc.addPyFile("bloomfilter.zip")
    rdd_input = sc.textFile(INPUT_PATH)

    # Parameter Calibration stage
    rdd_films = rdd_input.map(id_rate_split)
    counts_by_rating = rdd_films.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).sortByKey()

    # Bloom Filter Creation stage
    bf_params = [init_bloomfilter(n, p) for rating, n in counts_by_rating.collect()]
    broadcast_bf_params = sc.broadcast(bf_params)
    rdd_partial_bf = rdd_input.mapPartitions(insert_in_bloomfilters)
    rdd_final_bf = rdd_partial_bf.reduceByKey(lambda filter1, filter2: filter1.bitwise_or(filter2)).sortByKey()

    # Parameter Validation Stage
    broadcast_bf = sc.broadcast(rdd_final_bf.collect())
    rdd_counter = rdd_input.flatMap(validate_bloomfilter)
    rdd_false_positive_count = rdd_counter.reduceByKey(lambda x, y: x + y).sortByKey()

    with open(OUTPUT_PATH, "w") as f:
        for i in range(10):
            false_positive_rate = rdd_false_positive_count.collect()[i][1]/(rdd_input.count()-counts_by_rating.collect()[i][1])
            f.write(str(i+1) + "\t" + str(false_positive_rate) + "\n")

    execution_time = time.time() - start_time
    print("\nexecution time:", execution_time, "s")