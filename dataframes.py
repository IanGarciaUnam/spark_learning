from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DataFrame').getOrCreate()
df = spark.read.option("multiline","true").json('dwsample1-json.json')#Solved
df.show()

df.printSchema()
df.columns
#df.describe().show()

#Schema

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
schema =[StructField('edad', IntegerTyper(), True), StructField('nombre', StringType(),True)]
schema_final = StructType(fields = schema)
df = spark.read.json('dwsample1-json.json')
df.printSchema()
df.show()