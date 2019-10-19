import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import format_string, date_format

sc = SparkContext()
spark = SparkSession(sc)
parking = spark.read.format('csv').options(header='true', inferschema='true', delimiter=',').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
#total violations
result=spark.sql("SELECT violation_code, count(*) as total FROM parking GROUP BY violation_code")
result.createOrReplaceTempView("result")
#weekend violations
result2=spark.sql("SELECT violation_code, count(*) as weekend FROM parking WHERE issue_date IN ('2016-03-05 00:00:00','2016-03-06 00:00:00','2016-03-12 00:00:00','2016-03-13 00:00:00','2016-03-19 00:00:00','2016-03-20 00:00:00','2016-03-26 00:00:00','2016-03-27 00:00:00') GROUP BY violation_code")
result2.createOrReplaceTempView("result2")
#join and replace null with 0
result3=spark.sql("SELECT result.violation_code, case when result2.weekend is null then '0' else result2.weekend end as weekend, result.total as total FROM result LEFT OUTER JOIN result2 ON result.violation_code=result2.violation_code")
result3.createOrReplaceTempView("result3")
#calculate average
final=spark.sql("SELECT result3.violation_code, result3.weekend/8.00 as temp1, (result3.total-result3.weekend)/23.00 as temp2 FROM result3")
final.createOrReplaceTempView("final")
#round
final=spark.sql("SELECT violation_code, ROUND(temp1,2) as avg1, ROUND(temp2,2) as avg2 FROM final ORDER BY violation_code")
final.select(format_string('%s\t%s, %s',final.violation_code,final.avg1,final.avg2)).write.save("task7-sql.out",format="text")