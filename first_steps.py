from pyspark import SparkContext
sc = SparkContext()
textFile = sc.textFile('ejemplo.txt')

"""
RDD 
A Resilient Distributed  Dataset, basic abstraction in Spark. Represents an immutable, 
partitioned collection of elements that can be operated on in parallel

RDD.glom()

Return RDD created by coalescing all elements within each partition into a list

RDD.collect()
apply actions to RDD


"""

print(textFile)#Checking if textFile exists

#Acciones
textFile.collect()# Lista RDD
textFile.count() #Cuenta no. de elementos
textFile.first()

#Transformacion 

segunda = textFile.filter(lambda linea: 'segunda' in linea)
print(segunda) # Textfile filter object
x=segunda.collect() #Aplicamos accion
print(x)




