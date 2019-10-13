#!/usr/bin/env python
import sys
import csv

reader=csv.reader(sys.stdin,delimiter=',')
#22 columns for parking and 18 columns for open
for words in reader:
	if len(words)==22:
		print ("{0}\t{1}={2}={3}={4}={5}".format(words[0],words[14],words[6],words[2],words[1],'parking'))
	if len(words)==18:
		print ("{0}\t{1}={2}={3}={4}={5}".format(words[0],words[1],words[5],words[7],words[9],'open'))