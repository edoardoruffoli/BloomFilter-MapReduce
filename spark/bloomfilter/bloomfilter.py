import numpy as np
from pyspark import SparkContext


def array_split(line):
    np.array(line.split("/t"))


sc = SparkContext(appName="Bloomfilter", master="local[*]")

rdd_file = sc.textFile("film-ratings.txt").map(array_split)
print(rdd_file.take(4))