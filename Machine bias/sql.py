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
        .appName("sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    # get command-line arguments
    inFile = sys.argv[1]
    supp = sys.argv[2]
    conf = sys.argv[3]
    prot = sys.argv[4]

    print ("Executing SQL with input from " + inFile + ", support=" +supp + ", confidence=" + conf + ", protection=" + prot)

    pp_schema = StructType([
            StructField("uid", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("val", IntegerType(), True)])

    Pro_Publica = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
    Pro_Publica.createOrReplaceTempView("Pro_Publica")
    sqlContext.cacheTable("Pro_Publica")
    spark.sql("select count(*) from Pro_Publica").show()

    # compute frequent itemsets of size 1, store in F1(attr, val)
    query = "select attr, val, count(*) as supp \
	from Pro_Publica \
	group by attr, val \
	having count(*) >= "  + str(supp);
    F1 = spark.sql(query);
    F1.createOrReplaceTempView("F1")

    # YOUR SparkSQL CODE GOES HERE
    # You may use any valid SQL query, and store the output in intermediate temporary views
    # Output must go into R2, R3 and PD_R3 as stated below.  Do not change the format of the output
    # on the last three lines.
    query = "select a.attr as attr1, a.val as val1, b.attr as attr2, b.val as val2 from F1 a, F1 b where a.attr < b.attr";
    C2 = spark.sql(query);
    C2.createOrReplaceTempView("C2")
 
    query = "select attr1, val1, attr2, val2, count(P1.attr) as supp \
	from C2, Pro_Publica P1, Pro_Publica P2 \
	where P1.uid=P2.uid and \
	C2.attr1=P1.attr and \
	C2.val1=P1.val and \
	C2.attr2=P2.attr and \
	C2.val2=P2.val \
	group by attr1, val1, attr2, val2 \
	having count(P1.attr) >= "  + str(supp);
    F2 = spark.sql(query);
    F2.createOrReplaceTempView("F2")
    # Compute R2, as described in the homework specification
    # R2(attr1, val1, attr2, val2, supp, conf)
    query = "select attr1, val1, attr2, val2, F2.supp, F2.supp/F1.supp as conf \
	from F2,F1 \
	where F2.attr1=F1.attr and \
	F2.val1=F1.val and \
	attr2='vdecile' and \
	(F2.supp/F1.supp) >= "  + str(conf);
    R2 = spark.sql(query)
    R2.createOrReplaceTempView("R2")

    # MORE OF YOUR SparkSQL CODE GOES HERE
    query = "select Distinct a.attr1 as attr1, a.val1 as val1, a.attr2 as attr2, a.val2 as val2, b.attr2 as attr3, b.val2 as val3\
	from F2 a, F2 b \
	where a.attr1=b.attr1 and \
	a.val1=b.val1 and \
	a.attr2 < b.attr2";
    C3 = spark.sql(query);
    C3.createOrReplaceTempView("C3")
 
    query = "select attr1, val1, attr2, val2, attr3, val3, count(P1.attr) as supp \
	from C3, Pro_Publica P1, Pro_Publica P2, Pro_Publica P3 \
	where P1.uid=P2.uid and \
	P2.uid=p3.uid and \
	C3.attr1=P1.attr and \
	C3.val1=P1.val and \
	C3.attr2=P2.attr and \
	C3.val2=P2.val and \
	C3.attr3=P3.attr and \
	C3.val3=P3.val \
	group by attr1, val1, attr2, val2, attr3, val3 \
	having count(P1.attr) >= "  + str(supp);
    F3 = spark.sql(query);
    F3.createOrReplaceTempView("F3")
    # Compute R3, as described in the homework specification
    # R3(attr1, val1, attr2, val2, attr3, val3, supp, conf)
    query = "select F3.attr1, F3.val1, F3.attr2, F3.val2, F3.attr3, F3.val3, F3.supp, F3.supp/F2.supp as conf \
	from F3,F2 \
	where F3.attr1=F2.attr1 and \
	F3.val1=F2.val1 and \
	F3.attr2=F2.attr2 and \
	F3.val2=F2.val2 and \
	F3.attr1!='vdecile' and \
	F3.attr2!='vdecile' and \
	F3.attr3='vdecile' and \
	(F3.supp/F2.supp) >= "  + str(conf);
    R3 = spark.sql(query)
    R3.createOrReplaceTempView("R3")

    # MORE OF YOUR SparkSQL CODE GOES HERE

    # Compute PD_R3, as described in the homework specification
    # PD_R3(attr1, val1, attr2, val2, attr3, val3, supp, conf, prot)
    query = "select R3.attr1, R3.val1, R3.attr2, R3.val2, R3.attr3, R3.val3, R3.supp, R3.supp/F2.supp as conf, R3.conf/R2.conf as prot \
	from R3, R2, F2 \
	where F2.attr1=R3.attr1 and \
	F2.val1=R3.val1 and \
	F2.attr2=R3.attr2 and \
	F2.val2=R3.val2 and \
	R3.attr1=R2.attr1 and \
	R3.val1=R2.val1 and \
	R3.attr3=R2.attr2 and \
	R3.val3=R2.val2 and \
	F2.attr2='race' and \
	R3.attr1!='vdecile' and \
	R3.attr2='race' and \
	R3.attr3='vdecile' and \
	(R3.supp/F2.supp) >= "  + str(conf) + " and (R3.conf/R2.conf) >= " + str(prot) + " and R3.supp >= " + str(supp);
    PD_R3 = spark.sql(query)
    PD_R3.createOrReplaceTempView("PD_R3")

    R2.select(format_string('%s,%s,%s,%s,%d,%.2f',R2.attr1,R2.val1,R2.attr2,R2.val2,R2.supp,R2.conf)).write.save("r2.out",format="text")
    R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f',R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf)).write.save("r3.out",format="text")
    PD_R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f',PD_R3.attr1,PD_R3.val1,PD_R3.attr2,PD_R3.val2,PD_R3.attr3,PD_R3.val3,PD_R3.supp,PD_R3.conf,PD_R3.prot)).write.save("pd-r3.out",format="text")

    sc.stop()
 
