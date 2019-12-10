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
from pyspark.sql.functions import lit


def  predict_category(value):
    phone_pattern = r"^\s*(\+?(\d{1,3}))?[-. (]*\(?\d{3}\)?[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"
    lat_lon_cord_pattern = r"^\s*\(?-?(\d{0,3}\.\d),\s*-?(\d{0,3}\.\d)\)\s*$"
    zip_code_pattern = r"^\s*\d{5}-?(\d{4})?\s*$"
    building_classification_pattern = r"^\s*[A-Za-z]\d{1}-[\w-]*\s*$"
    website_pattern = r"^\s*(WWW\.|HTTP(s)?://)?.*(\.NET|\.ORG|\.COM)?/?\s*$"

    if re.match(phone_pattern, value):
        return "phone_number"
    elif re.match(lat_lon_cord_pattern, value):
        return "lat_lon_cord"
    elif re.match(zip_code_pattern, value):
        return "zip_code"
    elif re.match(website_pattern, value):
        return "website"
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
        if file == "h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz":
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
                building_classification = spark.sql("select _c0 from col where _c0 rlike '^\\\\s*[A-Za-z]\\\\d{1}-[\\\\w-]*\\\\s*$'")
                #building_classification.show()
                if building_classification.count() != col_count:
                    building_classification.createOrReplaceTempView("building_classification")
                    check = spark.sql("select _c0 from col except (select _c0 from building_classification)")
                    check = check.withColumn('prediction', predict('_c0'))
            #website
            elif re.match(r"^\w*website\w*", fileName):
                website = spark.sql("select _c0 from col where _c0 rlike '^\\\\s*(WWW\\\\.|HTTP(s)?://)?.*(\\\\.NET|\\\\.ORG|\\\\.COM)?/?\\\\s*$'")
                #website.show()
                #print("website count: ", website.count())
                if website.count() != col_count:
                    website.createOrReplaceTempView("website")
                    check = spark.sql("select _c0 from col except (select _c0 from website)")
                    check = check.withColumn('prediction', predict('_c0'))
                    #check.show(check.count(), False)
            #school_level
            elif re.match(r"^\w*school\w*level\w*", fileName):
                #school_level = spark.sql("select _c0 from col")
                school_level = spark.sql("select _c0 from col where UPPER(_c0) in ('K-2', 'K-3', 'K-8', 'ELEMENTARY', 'MIDDLE', 'HIGH SCHOOL TRANSFER', 'HIGH SCHOOL', 'D75','YABC')")
                school_level.show()
                print("school_level count: ", school_level.count())
                if school_level.count() != col_count:
                    school_level.createOrReplaceTempView("school_level")
                    check = spark.sql("select _c0 from col except (select _c0 from school_level)")
                    check = check.withColumn('prediction', predict('_c0'))
                    #check.show(check.count(), False)
            #city_agency
            elif re.match(r"^\w*city\w*agency\w*", fileName):
                #city_agency = spark.sql("select _c0 from col")
                city_agency = spark.sql("select _c0 from col where UPPER(_c0) in ('311','ACS','BIC','BOE','BPL','CCHR','CCRB','CUNY','DCA','DCAS','DCLA','DCP','DDC','DEP','DFTA',\
				'DHS','DOB','DOC','DOE','DOF','DOHMH','DOI','DOITT','DOP','DOR','DOT','DPR','DSNY','DVS','DYCD','EDC',\
				'FDNY','HPD','HRA','LAW','LPC','NYCEM','NYCHA','NYPD','NYPL','OATH','OCME','QPL','SBS','SCA','TLC')")
                city_agency.show()
                print("city_agency count: ", city_agency.count())
                if city_agency.count() != col_count:
                    city_agency.createOrReplaceTempView("city_agency")
                    check = spark.sql("select _c0 from col except (select _c0 from city_agency)")
                    check = check.withColumn('prediction', predict('_c0'))
                    #check.show(check.count(), False)
            #color
            elif re.match(r"^\w*color\w*", fileName):
                #color = spark.sql("select _c0 from col")
                color = spark.sql("select _c0 from col where UPPER(_c0) in ('BK', 'BL', 'BG', 'BR', 'GL', 'GY', 'MR', 'OR', 'PK', 'PR', 'RD', 'TN', 'WH', 'YW', \
				'BLACK', 'BLUE', 'BEIGE', 'BROWN', 'GOLD', 'GRAY', 'MAROON', 'ORANGE', 'PINK', 'PURPLE', 'RED', 'TAN', 'WHITE', 'YELLOW')")
                color.show()
                print("color count: ", color.count())
                if color.count() != col_count:
                    color.createOrReplaceTempView("color")
                    check = spark.sql("select _c0 from col except (select _c0 from color)")
                    check = check.withColumn('prediction', predict('_c0'))
                    #check.show(check.count(), False)
			#vehicle_type
            elif re.match(r"^\w*vehicle\w*type\w*", fileName):
                #vehicle_type = spark.sql("select _c0 from col")
                vehicle_type = spark.sql("select _c0 from col where UPPER(_c0) in ('FIRE', 'CONV', 'SEDN', 'SUBN', '4DSD', '2DSD', 'H/WH', 'ATV', 'MCY', 'H/IN', 'LOCO', 'RPLC',\
				'AMBU', 'P/SH', 'RBM', 'R/RD', 'RD/S', 'S/SP', 'SN/P', 'TRAV', 'MOBL', 'TR/E', 'T/CR', 'TR/C', 'SWT',\
				'W/DR', 'W/SR', 'FPM', 'MCC', 'EMVR', 'TRAC', 'DELV', 'DUMP', 'FLAT', 'PICK', 'STAK', 'TANK',\
				'REFG', 'TOW', 'VAN', 'UTIL', 'POLE', 'BOAT', 'H/TR', 'SEMI', 'TRLR', 'LTRL', 'LSVT', 'BUS', 'LIM',\
				'HRSE', 'TAXI', 'DCOM', 'CMIX', 'MOPD', 'MFH', 'SNOW', 'LSV')")
                vehicle_type.show()
                print("vehicle_type count: ", vehicle_type.count())
                if vehicle_type.count() != col_count:
                    vehicle_type.createOrReplaceTempView("vehicle_type")
                    check = spark.sql("select _c0 from col except (select _c0 from vehicle_type)")
                    check = check.withColumn('prediction', predict('_c0'))
                    #check.show(check.count(), False)
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
        
