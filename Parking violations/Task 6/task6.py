import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line = sc.textFile(sys.argv[1], 1)
line = line.mapPartitions(lambda x: reader(x))
#order by total_number in descending order and then plate_id in ascending order
count = line.map(lambda x: ((x[14],x[16]),1)).reduceByKey(add).takeOrdered(20, key  = lambda x: (-x[1], x[0]))
result = sc.parallelize(count).map(lambda x: str(x[0]).replace("'","").replace('(','').replace(')','') + '\t' + str(x[1])).saveAsTextFile("task6.out")
sc.stop()