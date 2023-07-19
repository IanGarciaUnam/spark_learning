from pyspark import SparkContext
from pyspark import StorageLevel
sc = SparkContext()

rdd = sc.parallelize(range(100),10)
print(rdd.is_cached)

level_storage=rdd.getStorageLevel()
print(level_storage)

rdd2 = rdd.map(lambda x: x**2)
rdd2.persist(StorageLevel.MEMORY_AND_DISK_2)
print(rdd2.getStorageLevel())

#Persistencia no se hereda

rdd3 = rdd2.map(lambda x: x*2)
print(rdd3.is_cached,rdd2.is_cached)
#unpersist() to quit cached