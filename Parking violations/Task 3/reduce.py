#!/usr/bin/env python
import sys
import os
import string

currKey=None
currAmount=0
count=0

for line in sys.stdin:
	line=line.strip()
	key,amount=line.split('\t')
	
	try:
		amount=float(amount)
	except ValueError:
		continue

	#if on the same key
	if currKey==key:
		currAmount+=amount
		count+=1
	#if on a new key
	else:
		if currKey:
			#total and average are rounded to 2 decimal places
			print ('{0}\t{1:.2f}, {2:.2f}'.format(currKey,round(currAmount,2),round(currAmount/count,2)))
		currKey=key	
		currAmount=amount
		count=1
#the last key
if currKey==key:
	#total and average are rounded to 2 decimal places
	print ('{0}\t{1:.2f}, {2:.2f}'.format(currKey,round(currAmount,2),round(currAmount/count,2)))