from pyspark import SparkContext
sc = SparkContext()
textFile = sc.textFile('ejemplo.txt')

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




