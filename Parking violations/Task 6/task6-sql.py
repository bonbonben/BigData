import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import format_string, date_format, desc

sc = SparkContext()
spark = SparkSession(sc)
parking = spark.read.format('csv').options(header='true', inferschema='true', delimiter=',').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
result=spark.sql("SELECT plate_id, registration_state, count(*) as num FROM parking GROUP BY plate_id, registration_state")
result.createOrReplaceTempView("result")
result=spark.sql("SELECT plate_id, registration_state, num FROM result ORDER BY num desc,plate_id LIMIT 20")
result.select(format_string('%s, %s\t%s',result.plate_id,result.registration_state,result.num)).write.save("task6-sql.out",format="text")