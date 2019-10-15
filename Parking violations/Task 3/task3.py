import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line = sc.textFile(sys.argv[1], 1)
line = line.mapPartitions(lambda x: reader(x))
type = line.map(lambda line: (line[2], line[12])).sortByKey()
type = type.groupByKey().map(lambda x : (x[0], list(x[1])))

def f(x):
	total=0
	average=0
	count=0
	for i in range (0, len(x)):
		total+=float(x[i])
		count+=1
	average=float(total/count)
	return total, average
	
result = type.map(lambda x: (x[0], f(x[1])))
result.map(lambda x: "%s\t%.2f, %.2f" %(x[0],x[1][0], x[1][1])).saveAsTextFile("task3.out")
sc.stop()