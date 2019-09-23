from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: wordcount <file>", file=sys.stderr)
		exit(-1)
	sc = SparkContext()
	lines = SparkContext()
	counts = lines.flatMap(lambda x:x.split(' ')) \
		.map(lambda x: (x,1)) \
		.reduceByKey(add)
	counts.saveAsTextFile("wc.out")

	sc.stop()
