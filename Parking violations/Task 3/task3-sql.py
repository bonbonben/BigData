import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import format_string, date_format

sc = SparkContext()
spark = SparkSession(sc)
open = spark.read.format('csv').options(header='true', inferschema='true', delimiter=',').load(sys.argv[1])
open.createOrReplaceTempView("open")
result=spark.sql("SELECT license_type, sum(amount_due) as s, avg(amount_due) as a FROM open GROUP BY license_type")
result.select(format_string('%s\t%.2f, %.2f',result.license_type,result.s,result.a)).write.save("task3-sql.out",format="text")