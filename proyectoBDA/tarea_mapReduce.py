import MapReduce
import sys
from pymongo import MongoClient
from time import time

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

# instancia del objeto MapReduce
mr = MapReduce.MapReduce()

#se almacenarán todos los artistas que arroje la funcion de mapReduce
artistas=[]
#se almacenarán la cantidad de canciones que ha creado cada artista de acuerdo al resultado que arroje la funcion de mapReduce
cantidadCanciones=[]

canciones=[]

def mapper(record):
    # key: document identifier - artista
    # value: document contents - cancion que pertenece al artista
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key,value)


def reducer(key, list_of_values):
    
    # --- TU CODIGO AQUI ---
    #se quita las canciones que salen repetidas para obtener datos unicos
    value2=[]
    for cancion in list_of_values:
        if(cancion not in value2):
            value2.append(cancion)
    #se imprime el nombre del cantante con la lista de canciones que ha creado       
    mr.emit((key,value2))
    
    artistas.append(key)
    cantidadCanciones.append(len(value2))
    canciones.append(value2)
    #se imprime el nombre del cantante con la cantidad de canciones que ha creado
    mr.emit((key,len(value2)))

if __name__ == '__main__':
    #cargamos nuestra colleccion Spotify de la base de datos de MongoDB al programa
    #filtramos la información, de tal forma que solo seleccionaremos las músicas que fueron 
    # lanzadas en el año 2020;
    # una popularidad mayor a 30 y
    # porcentaje bailable mayor a 0.8
    result= db[collectionName].find({
        "year":{ "$gte": 2020 },
        "popularity":{ "$gt": 30 },
        "danceability":{ "$gt": 0.8 }
    })
    # En la variable result obtenmos un objeto tipo mongo, por lo que se debe recorrer ese objeto para ir agregando
    # su informacion a una lista
    # print (result)
    #Generaremos un archivo tipo .json que sera enviado a la funcion de mapReduce
    f= open("datagenerada.json","w+")
    cantidad_contador=0
    for i in result:
        cantidad_contador=cantidad_contador+1
        columna_artistas=i['artists']
        e=columna_artistas[1:-1] #quitamos el simbolo "[" al inicio y "]" al final
        array_artistas=e.split(",") #dividimos el string por "," para obtener cada artista
        # por cada artista se limpiara las comillas simples
        for artista in array_artistas:
            nombre_sin_comilla=artista[1:-1]
            if (nombre_sin_comilla.find("'") !=-1):
                nombre_sin_comilla=artista[2:-1]
            # generamos el archivo de la data
            f.write("[\""+nombre_sin_comilla+"\",\""+i['name']+"\"]\n")
    #cerramos el archivo utilizado
    print("El resultado de la búsqueda realizada fue de: " +str(cantidad_contador))
    f.close
    # Mandamos a ejecutar el Map Reduce
    inputdata = open("datagenerada.json")
    
    # Se toma el tiempo antes de ejecutar la funcion MapReduce
    start_time = time()
    # Se ejecuta la función MapReduce
    mr.execute(inputdata, mapper, reducer)

    #Se toma el tiempo al finalizar la funcion MapReduce
    elapsed_time = time() - start_time
    # Se imprime el tiempo transcurrido
    print("\n\n**************************TIEMPO TRANSCURRIDO FUNCION MAPREDUCE****************\n")
    print("Elapsed time: %.10f seconds." % elapsed_time)
    print("\n\n*******************************************************************************\n")

# Se obtiene el valor maximo de la lista de cantidad de Canciones
maximo=max(cantidadCanciones)

# Se obtiene el índice del valor mayor de la lista de cantidad de canciones
indice= cantidadCanciones.index(maximo)

#se filtra las canciones que pertenecen al artista con mayor cantidad de canciones creadas
canciones_artista_ganador=canciones[indice]

print("El artista que más canciones ha creado es: "+ artistas[indice]+" con un total de "+str(maximo)+" canciones")
print("A continuación se muestra todas las canciones que ha creado: ")
contador=1

# Se muestran las canciones del artista ganador
for cancion in canciones_artista_ganador:
    print(str(contador) +".  "+ cancion)
    contador= contador+1
