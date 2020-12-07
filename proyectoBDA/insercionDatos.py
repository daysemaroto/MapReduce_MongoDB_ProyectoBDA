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

#insercion de datos a la base de datos
#se usa el comando insert_many para insertar varios datos a la vez
db[collectionName].insert_many([
    {"valence":0.0594,"year":2020,"acousticness":0.232,"artists":"['Camila Cabello', 'Shawn Mendez']","danceability":0.99,"duration_ms":74512,"energy":0.21100000000000002,"explicit":0,"id":"4BJqT0PrAfrxzMOxytFOIz","instrumentalness":0.878,"key":10,"liveness":0.665,"loudness":-20.096,"mode":1,"name":"Esta es una prueba","popularity":4,"release_date":"1921","speechiness":0.0366,"tempo":80.954},
    {"valence":0.0594,"year":2020,"acousticness":0.562,"artists":"['Lauren Jauregui', 'Steven Aoki']","danceability":0.75,"duration_ms":831667,"energy":0.21100000000000002,"explicit":0,"id":"4BJqT0PrAfrxzMOxytFOIz","instrumentalness":0.878,"key":10,"liveness":0.665,"loudness":-20.096,"mode":1,"name":"Esta es prueba 2","popularity":4,"release_date":"1921","speechiness":0.0366,"tempo":80.954}
])
print("Inserción culminada con éxito")

