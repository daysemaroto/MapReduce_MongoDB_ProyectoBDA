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

#modificacion de datos a la base de datos
#update_one indica que solamente se actualizará un dato
db[collectionName].update_one(
    {"name": "Esta es una prueba"}, #condicion para hacer la modificacion
    {
        "$set":{
            "valence": 0.1,  #valores que se modificarán
            "year": 2021
        }
    }
)

db[collectionName].update_one(
    {"name": "Esta es prueba 2"},
    {
        "$set":{
            "valence": 0.7,
            "year": 2021
        }
    }
)

print("Modificación culminada con éxito")

