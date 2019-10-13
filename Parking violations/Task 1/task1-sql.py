import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import format_string, date_format

sc = SparkContext()
spark = SparkSession(sc)
parking = spark.read.format('csv').options(header='true', inferschema='true', delimiter=',').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
open = spark.read.format('csv').options(header='true', inferschema='true', delimiter=',').load(sys.argv[2])
open.createOrReplaceTempView("open")
result=spark.sql("SELECT summons_number, plate_id, violation_precinct, violation_code, issue_date FROM parking WHERE summons_number NOT IN (SELECT distinct(summons_number) FROM open)")
result.select(format_string('%d\t%s, %d, %d, %s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")