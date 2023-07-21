from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DataFrame').getOrCreate()
df = spark.read.option("multiline","true").json('dwsample1-json.json')#Solved
df.show()

df.printSchema()
df.columns
#df.describe().show()

#Schema

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
schema =[StructField('edad', IntegerType(), True), StructField('nombre', StringType(),True)]
schema_final = StructType(fields = schema)
df = spark.read.option("multiline","true").json('dwsample1-json.json')
df.printSchema()
df.show()

print(df)

rdd1=df.rdd

print(rdd1)
print(rdd1.getNumPartitions())