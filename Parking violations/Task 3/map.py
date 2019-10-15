#!/usr/bin/env python
import sys
import csv
import os
import string

reader=csv.reader(sys.stdin,delimiter=',')
#license_type is at the third column
for type in reader:
	key=type[2]
	
	try:
		value=float(type[12])
	except ValueError:
		continue

	print ('{0:s}\t{1:f}'.format(key,value))