import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line = sc.textFile(sys.argv[1], 1)
line = line.mapPartitions(lambda x: reader(x))
#key = violation_code
code = line.map(lambda line: (line[2]))
#calculate frequencies of violation code
result = code.map(lambda x: (x,1)).reduceByKey(add)
# map and save
result.map(lambda x: x[0] + '\t' + str(x[1])).saveAsTextFile("task2.out")
sc.stop()