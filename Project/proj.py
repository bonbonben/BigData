#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__ == "__main__":

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("proj") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    # get command-line arguments
    inFile = sys.argv[1]
    
    print ("Executing proj with input from " + inFile)

    spark = SparkSession(sc)
    test = sqlContext.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(sys.argv[1])
    test.createOrReplaceTempView("test")
    sqlContext.cacheTable("test")
    spark.sql("select * from test").show()
    spark.sql("select count(*) from test").show()
    
    columnNames = test.schema.names
	
    for columnName in columnNames:
        print("column name:", columnName)
        temp = spark.sql("SELECT `" + columnName + "` FROM test")
        temp.show()
        temp.createOrReplaceTempView("temp")
		
        temp1 = spark.sql("SELECT count(*) FROM temp WHERE `" + columnName + "` IS NOT NULL")
        temp1.show()
        temp2 = spark.sql("SELECT count(*) FROM temp WHERE `" + columnName + "` IS NULL")
        temp2.show()
        temp3 = spark.sql("SELECT count(distinct(`" + columnName + "`)) FROM temp")
        temp3.show()
        temp4 = spark.sql("SELECT `" + columnName + "`, COUNT(*) FROM temp GROUP BY `" + columnName + "` ORDER BY COUNT(*) DESC LIMIT 5")
        temp4.show()
        
		
    sc.stop()
