#!/usr/bin/env python
import sys
import csv

reader=csv.reader(sys.stdin,delimiter=',')
#violation code is at the third column
for words in reader:
	print ("{0}\t{1}".format(words[2],1))