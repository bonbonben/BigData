import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line = sc.textFile(sys.argv[1], 1)
line = line.mapPartitions(lambda x: reader(x))
task = line.map(lambda x: x[16])

def state(x):
	if x == 'NY':
		return ('NY', 1)
	else:
		return ('Other', 1)

count = task.map(lambda x : state(x))
result = count.reduceByKey(add)
result.map(lambda x: x[0] + '\t' + str(x[1])).saveAsTextFile("task4.out")
sc.stop()