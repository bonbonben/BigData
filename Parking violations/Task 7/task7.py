import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line = sc.textFile(sys.argv[1], 1)
line = line.mapPartitions(lambda x: reader(x))
code = line.map(lambda line: (line[2], line[1])).sortByKey()
code = code.groupByKey().map(lambda x : (x[0], list(x[1])))
    
def f(x):
    week = 0
    weekend = 0
    for i in range (0, len(x)):
        if x[i] in ['2016-03-05','2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27']:
            weekend+=1
        else:
            week+=1
    week_average = float(week/23.00)
    weekend_average = float(weekend/8.00)
    return weekend_average, week_average

result = code.map(lambda x: (x[0], f(x[1])))
result.map(lambda x: "%s\t%.2f, %.2f" %(x[0],x[1][0], x[1][1])).saveAsTextFile("task7.out")
sc.stop()