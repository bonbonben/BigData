#!/usr/bin/env python
import sys

currKey=None
currVal=None
prevKey=None

for line in sys.stdin:
	line=line.strip()
	key,value=line.split('\t',1)
	value=value.split('=')

	if currKey==key:
		prevKey=currKey
	else:
		if currKey:
			if currKey!=prevKey and currVal[-1]=='parking':
				print ("{0}\t{1}, {2}, {3}, {4}".format(currKey,currVal[0],currVal[1],currVal[2],currVal[3]))
		currKey=key
		currVal=value

if currKey!=prevKey and currVal[-1]=='parking':
	print ("{0}\t{1}, {2}, {3}, {4}".format(currKey,currVal[0],currVal[1],currVal[2],currVal[3]))