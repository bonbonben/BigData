#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string
import json

import dateutil.parser
from dateutil.parser import parse
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

def identify(value):
    try:
        check = float(value)
        if check.is_integer():
            return "INTEGER"
        elif check: 
            return "REAL"
    except:
        pass

    try:
        #s = str(value)
		
        s = dateutil.parser.parse(str(value))
        check = datetime.strptime(str(s),'%Y-%m-%d  %H:%M:%S')
        if check:
            return "DATE/TIME"
		
        #fmts = ["%m/%d/%Y %H:%M:%S %p", "%x", "%X"]
        #for fmt in fmts:
        #    check = datetime.strptime(s, fmt)
        #    if check:
        #        return "DATE/TIME"
    except:
        return "TEXT"

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
    outputJSON = dict()
    outputJSON['dataset_name'] = inFile
    print ("Executing proj with input from " + inFile)

    spark = SparkSession(sc)
    test = sqlContext.read.format('csv').options(header='true', inferschema='true', delimiter='\t').load(sys.argv[1])
    test.createOrReplaceTempView("test")
    sqlContext.cacheTable("test")
    #spark.sql("select * from test").show()
    #spark.sql("select count(*) from test").show()

    attrs = test.schema.names
    columns = []

    for attr in attrs:
        temp_column = dict()
        print("column name:", attr)
        temp_column['column_name'] = attr
        temp = spark.sql("SELECT `" + attr + "` as attr FROM test")
        #temp.show()
        temp.createOrReplaceTempView("temp")
        
        temp_not_null = spark.sql("SELECT attr FROM temp WHERE attr IS NOT NULL")
        temp_not_null.createOrReplaceTempView("temp_not_null")

        temp1 = spark.sql("SELECT count(*) as count FROM temp WHERE attr IS NOT NULL")
        #temp1.show()
        t1 = temp1.select("count").collect()[0][0]
        temp_column['number_non_empty_cells'] = t1;

        temp2 = spark.sql("SELECT count(*) as count FROM temp WHERE attr IS NULL")
        #temp2.show()
        t2 = temp2.select("count").collect()[0][0]
        temp_column['number_empty_cells'] = t2;

        temp3 = spark.sql("SELECT count(distinct(attr)) as count FROM temp")
        #temp3.show()
        t3 = temp3.select("count").collect()[0][0]
        temp_column['number_distinct_values'] = t3;
        
        temp4 = spark.sql("SELECT attr, COUNT(*) FROM temp GROUP BY attr ORDER BY COUNT(*) DESC LIMIT 5")
        #temp4.show()
        t4_array = [str(row.attr) for row in temp4.collect()]
        temp_column['frequent_values'] = t4_array;
        #print("array: ", t4_array)
        
        identify_types = udf(lambda x: identify(x))
        temp5 = temp_not_null.withColumn('types', identify_types('attr'))
        temp5.createOrReplaceTempView("temp5")
        temp6 = spark.sql("SELECT distinct(types) FROM temp5")
        temp6.show()

        types_array = [str(row.types) for row in temp6.collect()]
        types = []
        for type_ in types_array:
            temp_types = dict()
            if type_ == 'INTEGER':
                temp_types['type'] = 'INTEGER (LONG)'
                temp_int = spark.sql("SELECT attr, types FROM temp5 WHERE types = 'INTEGER'")
                #temp_real.createOrReplaceTempView("temp_real")
                to_int = udf(lambda x: int(float(x)))
                temp_int = temp_int.withColumn('attr', to_int('attr'))
                temp_int.createOrReplaceTempView("temp_int")
                temp5_int = spark.sql("SELECT count(*) as count, max(attr) as max, min(attr) as min, mean(attr) as mean, stddev(attr) as stddev FROM temp_int WHERE types = 'INTEGER'")
                temp5_int.show()
                
                t5 = temp5_int.select("count").collect()[0][0]
                temp_types['count'] = t5
                t5 = temp5_int.select("max").collect()[0][0]
                temp_types['max_value'] = t5
                t5 = temp5_int.select("min").collect()[0][0]
                temp_types['min_value'] = t5
                t5 = temp5_int.select("mean").collect()[0][0]
                temp_types['mean'] = t5
                t5 = temp5_int.select("stddev").collect()[0][0]
                temp_types['stddev'] = t5

            elif type_ == 'REAL':
                temp_types['type'] = 'REAL'
                temp_real = spark.sql("SELECT attr, types FROM temp5 WHERE types = 'REAL'")
                #temp_real.createOrReplaceTempView("temp_real")
                to_float = udf(lambda x: float(x))
                temp_real = temp_real.withColumn('attr', to_float('attr'))
                temp_real.createOrReplaceTempView("temp_real")
                temp5_real = spark.sql("SELECT count(*) as count, max(float(attr)) as max, min(float(attr)) as min, mean(attr) as mean, stddev(attr) as stddev FROM temp_real WHERE types = 'REAL'")
                temp5_real.show()
                
                t5 = temp5_real.select("count").collect()[0][0]
                temp_types['count'] = t5
                t5 = temp5_real.select("max").collect()[0][0]
                temp_types['max_value'] = t5
                t5 = temp5_real.select("min").collect()[0][0]
                temp_types['min_value'] = t5
                t5 = temp5_real.select("mean").collect()[0][0]
                temp_types['mean'] = t5
                t5 = temp5_real.select("stddev").collect()[0][0]
                temp_types['stddev'] = t5

            elif type_ == 'DATE/TIME':
                temp_types['type'] = 'DATE/TIME'
                temp5_date = spark.sql("SELECT count(*) as count, max(attr) as max_date, min(attr) as min_date FROM temp5 WHERE types = 'DATE/TIME'")
                t5 = temp5_date.select("count").collect()[0][0]
                temp_types['count'] = t5
                t5 = temp5_date.select("max_date").collect()[0][0]
                temp_types['max_value'] = t5
                t5 = temp5_date.select("min_date").collect()[0][0]
                temp_types['min_value'] = t5
                temp5_date.show()

            elif type_ == 'TEXT':
                temp_types['type'] = 'TEXT'
                temp5_text_s = spark.sql("SELECT attr, length(attr) as len FROM temp5 WHERE types = 'TEXT' ORDER BY len LIMIT 5")
                temp5_text_l = spark.sql("SELECT attr, length(attr) as len FROM temp5 WHERE types = 'TEXT' ORDER BY len DESC LIMIT 5")
                temp5_text = spark.sql("SELECT count(*) as count, avg(length(attr)) as len_ave FROM temp5 WHERE types = 'TEXT'")
                t5 = temp5_text.select("count").collect()[0][0]
                temp_types['count'] = t5
                t5_short_array = [str(row.attr) for row in temp5_text_s.collect()]
                temp_types['shortest_values'] = t5_short_array
                t5_long_array = [str(row.attr) for row in temp5_text_l.collect()]
                temp_types['longest_values'] = t5_long_array
                t5 = temp5_text.select("len_ave").collect()[0][0]
                temp_types['average_length'] = t5

                #temp5_text_s.show()
                #temp5_text_l.show()
                #temp5_text.show()
            types.append(temp_types)

        temp_column['data_types'] = types
        columns.append(temp_column)

    outputJSON['columns'] = columns
    #print(outputJSON)

    with open('test.json', 'w') as f:
        json.dump(outputJSON, f)
    #with open("test.json", "w+") as output:
    #    output.write(temp1.toJSON())

    sc.stop()