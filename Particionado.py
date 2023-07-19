from pyspark import SparkContext
sc = SparkContext()
rdd = sc.parallelize([1,1,2,2,3,3,4,4,6],4)
#getNumPartitions()
particiones = rdd.getNumPartitions()
print(particiones)
#using glom

collect=rdd.glom().collect()
print(collect)

#El particionado sí se hereda

rdd2= rdd.map(lambda x: x*2)
screen1= rdd2.glom().collect()
print(screen1)

#Cambiando particionado a traves de ReduceBYKey

pair_rdd=rdd.map(lambda x: (x,x))
pr=pair_rdd.glom().collect()
print(pr)

pr_reducedByKey=pair_rdd.reduceByKey(lambda x,y:x+y)
pr_k=pr_reducedByKey.glom().collect()
print(pr_k)

#Funciones de repartición
"""
-repartition ' devuelve un nuevo RDD que tiene n-particiones
-coalesce(n) ' solo permite reducir el número de particiones
-partitionBy(n, [partitionFunc]) - utilizando una función de partición para RDDs 
"""

pair_rdd6= pair_rdd.repartition(6)
numPartitions=pair_rdd6.getNumPartitions()
print(numPartitions, "no. particiones")
result= pair_rdd6.glom().collect()

pair_rdd2 = pair_rdd.coalesce(2)
show= pair_rdd2.glom().collect()
print(show)
pair_rdd3=pair_rdd.partitionBy(3)
show2=pair_rdd.glom().collect()
print(show2)