import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import format_string, date_format

sc = SparkContext()
spark = SparkSession(sc)
parking = spark.read.format('csv').options(header='true', inferschema='true', delimiter=',').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
result=spark.sql("SELECT violation_code, COUNT(*) as num FROM parking GROUP BY violation_code")
result.select(format_string('%d\t%s',result.violation_code,result.num)).write.save("task2-sql.out",format="text")