#!/usr/bin/env python
# coding: utf-8

import os, sys
import pyspark
import string
import json
from datetime import datetime
import dateutil.parser

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
    outputJSON = []

    for fileName in os.listdir("json"):
    	path = "json/" + fileName
        with open(path,'r') as f:
            temp_JSON = json.load(f)
        outputJSON.append(temp_JSON)

    with open('task1.json', 'w') as f:
        json.dump(outputJSON, f)
	sc.stop()