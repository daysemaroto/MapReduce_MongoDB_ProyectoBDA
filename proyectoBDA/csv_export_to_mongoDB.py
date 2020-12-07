try:
    import pymongo
    from pymongo import MongoClient
    import pandas as pd
    import json
except Exception as e:
    print("some Modules are Missing")


class MongoDB(object):
    def __init__(self, dBName=None, collectionName=None):
        self.dBName = dBName
        self.collectionName = collectionName
        self.client = MongoClient("localhost", 27017, maxPoolSize=50)
        self.DB = self.client[self.dBName]
        self.collection = self.DB[self.collectionName]

    def InsertData(self, path=None):
        # :param path: Path del archivo csv
        # :return: None

        df = pd.read_csv(path)
        data = df.to_dict('records')

        self.collection.insert_many(data, ordered=False)
        print("Toda la data ha sido exportada a Mongo DB Server")


if __name__ == "__main__":
    # se coloca un nombre para la base de datos de mongo, en este caso se llamara db_spotify
    mongodb = MongoDB(dBName="db_spotify", collectionName='Spotify')
    mongodb.InsertData(path="./data/data.csv")