import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line = sc.textFile(sys.argv[1], 1)
line = line.mapPartitions(lambda x: reader(x))
count = line.map(lambda x: ((x[14],x[16]),1)).reduceByKey(add).takeOrdered(1, key  = lambda x: -x[1])
result = sc.parallelize(count).map(lambda x: str(x[0]).replace("'","").replace('(','').replace(')','') + '\t' + str(x[1])).saveAsTextFile("task5.out")
sc.stop()