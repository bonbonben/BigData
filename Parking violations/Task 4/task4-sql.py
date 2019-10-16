import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import format_string, date_format

sc = SparkContext()
spark = SparkSession(sc)
parking = spark.read.format('csv').options(header='true', inferschema='true', delimiter=',').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
result1=spark.sql("SELECT COUNT(*) as num1 FROM parking WHERE registration_state=='NY'")
result2=spark.sql("SELECT COUNT(*) as num2 FROM parking WHERE registration_state!='NY'")
result1.select(format_string('NY\t%s',result1.num1)).write.save("task4-sql.out",format="text")
result2.select(format_string('Other\t%s',result2.num2)).coalesce(1).write.save("task4-sql.out",format="text",mode="append")