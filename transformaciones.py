#Transformaciones

from pyspark import SparkContext

sc = SparkContext()
lista=[1,2,3,4,5,6,7]

rdd= sc.parallelize(lista)
screen = rdd.collect()
print(screen)

# filter aplicado

filter_rdd= rdd.filter(lambda x: x>1)#eliminamos los mayores a 1
print(filter_rdd.collect())

#MAP aplicado

def suma_1(x):
    return x+1

map_suma1=filter_rdd.map(suma_1)
screen= map_suma1.collect()
print(screen)

cuadrado_rdd= (filter_rdd.map(suma_1).map(lambda x: (x,x**2)))#Devuelve el cuadrado de la lista previamente filtrada
screen = cuadrado_rdd.collect()
print(screen)
# Realizamos lo mismo pero utilizando flatMap
flat_cuadrado_rdd= (filter_rdd.map(suma_1).flatMap(lambda x: (x,x**2)))#Devuelve el cuadrado de la lista previamente filtrada
screen = flat_cuadrado_rdd.collect()
print(screen)