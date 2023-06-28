from pymongo import MongoClient
import pandas as pd
import re

class CMongoDb:
    def __init__(self, sFilePath, sUri="mongodb://localhost:27017", sDatabaseName='MyDB', sCollectionName='MyCollection'):
        self._client = MongoClient(sUri)
        self._db = self._client[sDatabaseName]
        self._sFilePath = sFilePath
        self._collection = self._db[sCollectionName]

    def DropCollectionAndCloseConnection(self, bDropCollectionFlag=True):
        if bDropCollectionFlag:
            self._collection.drop()
        self._client.close()

    @staticmethod
    def IsNotContainsSpecialCharacters(string):
        pattern = r'[^0-9,]'
        match = re.search(pattern, string)
        return match is None

    def InsertDocuments(self):

        df = pd.read_csv(self._sFilePath)
        # clean the data for non-numric types in value
        mask = df['Value'].apply(lambda x: self.IsNotContainsSpecialCharacters(x))
        df = df[mask]
        df['Value'] = df['Value'].str.replace(',', '').astype(int)

        data = df.to_dict(orient='records')
        return self._collection.insert_many(data)


    def FindDocuments(self):
        # Calculate sum of "Value" over levels 1 and 3 using MongoDB query

        lQuery = [
            {
                '$match': {
                    '$and': [
                        {
                            '$or': [
                                {'Industry_aggregation_NZSIOC': 'Level 1'},
                                {'Industry_aggregation_NZSIOC': 'Level 3'}
                            ]
                        },
                        {
                            'Units': {'$in': ['Dollars (millions)', 'Dollars']}
                        },
                        {
                            'Value': {'$type': 'number'}
                        }
                    ]
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_value': {
                        '$sum': {
                            '$cond': {
                                'if': {'$eq': ['$Units', 'Dollars']},
                                'then': {'$multiply': ['$Value', 0.000001]},
                                # Multiply by 10^-6 for conversion to millions
                                'else': '$Value'
                            }
                        }
                    }
                }
            }
        ]
        result = self._collection.aggregate(lQuery)
        if not result:
            print("No data found.")
            return
        nTotalValue = next(result)['total_value']

        print(f"Total value over levels 1 and 3: {nTotalValue} million dollars")
