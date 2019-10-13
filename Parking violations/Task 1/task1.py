import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1], 1)
line1 = line1.mapPartitions(lambda x: reader(x))
line1 = line1.filter(lambda line: len(line)>1).map(lambda line: (line[0], str(line[14]) + ', ' + str(line[6]) + ', ' + str(line[2]) + ', ' + str(line[1])))
line2 = sc.textFile(sys.argv[2],1)
line2 = line2.mapPartitions(lambda x: reader(x))
line2 = line2.filter(lambda line: len(line)>1).map(lambda line: (line[0], str(line[1]) + ', ' + str(line[5]) + ', ' + str(line[7]) + ', ' + str(line[9])))
result=line1.subtractByKey(line2)
result.map(lambda x:"\t".join([str(s) for s in x])).saveAsTextFile("task1.out")
sc.stop()