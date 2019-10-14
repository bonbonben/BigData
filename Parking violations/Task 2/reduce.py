#!/usr/bin/env python
import sys

count=0
currKey=None

for line in sys.stdin:
	line=line.strip()
	key,value=line.split('\t')

	if currKey==key:
		count=count+1
	else:
		if currKey:
			print ("{0}\t{1}".format(currKey,count))
		currKey=key
		count=1

if currKey==key:
	print ("{0}\t{1}".format(currKey,count))