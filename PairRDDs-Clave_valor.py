from pyspark import SparkContext
sc = SparkContext()
#Creamos un pair RDD a partir de una lista de tuplas

lista_tuplas= [('a',1), ('b',2), ('c',3)]
print(lista_tuplas)
pair_rdd = sc.parallelize(lista_tuplas)
screen = pair_rdd.collect()
print(screen)
#Utilizando zip()
pair_rdd2= sc.parallelize(zip((['a','b','c']), range(1,4,1)))
screen2=pair_rdd2.collect()
print(screen2)

#Using textfile
rdd_textfile= sc.textFile("ejemplo.txt")
pair_rdd_textfile=rdd_textfile.map(lambda x: (x.split(" ")[0],x))
screen3_textfile=pair_rdd_textfile.collect()
print(screen3_textfile)
#Para escoger algunos
screen3_takeSamples=pair_rdd_textfile.takeSample(False, 2)
print(screen3_takeSamples)


#Utilizando keyby()
rdd = sc.parallelize(range(5))
screen4= rdd.collect()
print(screen4)

pair_rdd=rdd.keyBy(lambda x: x+1)
screen4_keyBy=pair_rdd.collect()
print(screen4_keyBy)


#Utilizando zipWithIndex()
rdd = sc.parallelize(['a','b','c','d','e'])
pair_rdd= rdd.zipWithIndex()
screen5UsingZip = pair_rdd.collect()
print(screen5UsingZip)

#Utilizando zipWithUniqueId
screen6 = rdd.zipWithUniqueId().collect()
print(screen6)

#Utilizando zipWithUniqueId and glom
screen7= rdd.zipWithUniqueId().glom().collect()
print(screen7)

#Utilizando zip con dos rdd's
rdd1=sc.parallelize(range(5),3)
rdd2=sc.parallelize(range(100,105,1),3)
screen8=rdd1.collect()
pair_rdd=rdd1.zip(rdd2)
screen9=pair_rdd2.collect()
print(screen8)
print(screen9)