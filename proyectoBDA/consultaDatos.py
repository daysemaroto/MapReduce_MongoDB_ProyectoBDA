from pymongo import MongoClient
#representa la direccion ip del localhost
ip='127.0.0.1'
# 27017 is the default port number for mongodb
port=27017

#nombre de la base de datos
dbName="db_spotify"

#nombre de la coleccion que vamos a manipular dentro de nuestra base de datos
collectionName= 'Spotify'

#conexion con mongoDB
client= MongoClient(ip,port)
db=client[dbName]

#lectura de datos a la base de datos
#almacenamos los datos en una variabla para luego proceder a imprimirlo por consola.
result= db[collectionName].find({"name": "Esta es una prueba"})

print("******************Primera consulta************")
for i in result:
    print(i)

result2= db[collectionName].find({"valence": { "$gt": 0.99 }})
print("")
print("******************Segunda consulta************")
for j in result2:
    print(j)

print("Lecturas culminadas con éxito")

