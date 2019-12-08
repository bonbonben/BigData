#!/usr/bin/env python
# coding: utf-8

import os, sys
import pyspark
import string
import json
import time
import dateutil.parser

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__ == "__main__":
    start_time = time.time()
    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("proj") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    outputJSON = []
    #inFile = sys.argv[1]

    with open("cluster3.txt", 'r') as f:
        cols = f.read().replace('\n', '').replace(' ', '').replace("'", '')
    
    cols = cols[1:-1]
    file_list = cols.split(',')
    #print("file list: ", file_list)

    file_id = 0
    path = '/user/hm74/NYCColumns/'
    
    labels = dict()
    for file in file_list:
        if file_id < 135 and file != '4d7f-74pe.Address.txt.gz' and file != '64gx-bycn.EMPCITY.txt.gz' and file != 'pq5i-thsu.DVC_MAKE.txt.gz' and file != 'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz' and file != 'tukx-dsca.Address_1.txt.gz' and file != 'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz' and file != 'mdcw-n682.First_Name.txt.gz' and file != 'sxx4-xhzg.Park_Site_Name.txt.gz':
            inFile = path + file
            print("File Name: ", file)
            file_name = file.split('.')[1]

            if file in labels:
                labels[file].append(file_name)
            else:
                labels[file] = [file_name]

            spark = SparkSession(sc)
            col = sqlContext.read.format('csv').options(inferschema='true', delimiter='\t').load(inFile)
            col.createOrReplaceTempView("col")
            sqlContext.cacheTable("col")
            spark.sql("select * from col").show(col.count(), False)
            file_id += 1
        else:
            file_id += 1

    with open('135_label.json', 'w', encoding='utf-8') as f:
        json.dump(labels, f, ensure_ascii=False)
	
    sc.stop()