#!/usr/bin/env python
# coding: utf-8

import os, sys
import pyspark
import string
import json
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import lower, col

def  predict_category(value):
    phone_pattern = r"^\s*(\+?(\d{1,3}))?[-. (]*\(?\d{3}\)?[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"
    lat_lon_cord_pattern = r"^\s*\(?-?(\d{0,3}\.\d),\s*-?(\d{0,3}\.\d)\)\s*$"
    zip_code_pattern = r"^\s*\d{5}-?(\d{4})?\s*$"
    building_classification_pattern = r"^\s*[A-Za-z]\d{1}-.*\s*$"
    website_pattern = r"^\s*(WWW\.|HTTP(s)?://)?.*(\.NET|\.ORG|\.COM)?/?\s*$"
    
    if re.match(phone_pattern, value):
        return "phone_number"
    if re.match(lat_lon_cord_pattern, value):
        return "lat_lon_cord"
    if len(str(value)) >= 5 and re.match(zip_code_pattern, value):
        return "zip_code"
    if len(str(value)) >= 4 and re.match(website_pattern, value):
        return "website"

    if value in borough_list:
        return "borough"
    else:
        return "other"


if __name__ == "__main__":

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
    
    output = []
    for file in file_list:
        temp_output = dict()
        if file == "6ypq-ih9a.BOROUGH.txt.gz":
            inFile = path + file
            print("File Name: ", file)
            fileName = file.split('.')[1]
            fileName = fileName.lower()

            spark = SparkSession(sc)
            col = sqlContext.read.format('csv').options(inferschema='true', delimiter='\t').load(inFile)
            col.createOrReplaceTempView("col")
            sqlContext.cacheTable("col")
            #spark.sql("select * from col").show(col.count(), False)
            col_count = col.count()
            print("Total count: ", col_count)

            borough_list = ["K", "M", "Q", "R", "X", "BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]
            lst = [x.lower() for x in borough_list]
            borough_list = borough_list + lst
            boro = spark.createDataFrame(borough_list, StringType())
            boro.createOrReplaceTempView("boro")

            predict = udf(lambda x: predict_category(x))

            #phone
            if re.match(r"^\w*phone|fax\w*", fileName):
                phone = spark.sql("select _c0 from col where _c0 rlike '^\\\\s*(\\\\+?(\\\\d{1,3}))?[-. (]*\\\\(?\\\\d{3}\\\\)?[-. )]*(\\\\d{3})[-. ]*(\\\\d{4})(?: *x(\\\\d+))?\\\\s*$'")
                #phone.show()
                if phone.count() != col_count:
                    phone.createOrReplaceTempView("phone")
                    check = spark.sql("select _c0 from col except (select _c0 from phone)")
                    check = check.withColumn('prediction', predict('_c0'))
            #lat_lon_cord
            elif re.match(r"^\w*location\w*", fileName):
                lat_lon_cord = spark.sql("select _c0 from col where _c0 rlike '^\\\\s*\\\\(?-?(\\\\d{0,3}\\\\.\\\\d*),\\\\s*-?(\\\\d{0,3}\\\\.\\\\d*)\\\\)\\\\s*$'")
                #lat_lon_cord.show()
                if lat_lon_cord.count() != col_count:
                    lat_lon_cord.createOrReplaceTempView("lat_lon_cord")
                    check = spark.sql("select _c0 from col except (select _c0 from lat_lon_cord)")
                    check = check.withColumn('prediction', predict('_c0'))
            #zip_code
            elif re.match(r"^\w*zip\w*", fileName):
                zip_code = spark.sql("select _c0 from col where _c0 rlike '^\\\\s*\\\\d{5}-?(\\\\d{4})?\\\\s*$'")
                #zip_code.show()
                if zip_code.count() != col_count:
                    zip_code.createOrReplaceTempView("zip_code")
                    check = spark.sql("select _c0 from col except (select _c0 from zip_code)")
                    check = check.withColumn('prediction', predict('_c0'))
            #building_classification
            elif re.match(r"^\w*building\w*classification\w*", fileName):
                building_classification = spark.sql("select _c0 from col where _c0 rlike '^\\\\s*[A-Za-z]\\\\d{1}-.*\\\\s*$'")
                #building_classification.show()
                if building_classification.count() != col_count:
                    building_classification.createOrReplaceTempView("building_classification")
                    check = spark.sql("select _c0 from col except (select _c0 from building_classification)")
                    check = check.withColumn('prediction', predict('_c0'))
            #website
            elif re.match(r"^\w*website\w*", fileName):
                website = spark.sql("select _c0 from col where _c0 rlike '^\\\\s*(WWW\\\\.|HTTP(s)?://)?.*(\\\\.NET|\\\\.ORG|\\\\.COM)?/?\\\\s*$'")
                #website.show()
                if website.count() != col_count:
                    website.createOrReplaceTempView("website")
                    check = spark.sql("select _c0 from col except (select _c0 from website)")
                    check = check.withColumn('prediction', predict('_c0'))
            #borough
            elif re.match(r"^\w*boro\w*", fileName):
                borough = spark.sql("select c._c0 from col c, boro b where c._c0 = b.value")
                #borough.show()
                if borough.count() != col_count:
                    borough.createOrReplaceTempView("borough")
                    check = spark.sql("select _c0 from col except (select _c0 from borough)")
                    check = check.withColumn('prediction', predict('_c0'))
            else:
                check = col.withColumn('prediction', predict('_c0'))
                check.show(check.count(), False)


            """
            if check.count() != 0:
                

            """
        #else:
            #file_id += 1

    #with open('135_label.json', 'w') as f:
        #json.dump(labels, f)
