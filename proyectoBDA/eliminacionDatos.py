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

#eliminación de datos a la base de datos
db[collectionName].delete_many(
    {"name":"Esta es una prueba"}
)

db[collectionName].delete_many(
    {"year": 2021}
)

print("Eliminación culminada con éxito")

