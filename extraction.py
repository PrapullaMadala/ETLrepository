from urllib.request import urlopen
import pandas as pd
import json
from sqlalchemy import create_engine
from transformation import Transform


class Extract:
    def __init__(self, datasource, dataset, data):
        print(self)
        self.datasource = datasource
        print(self.datasource)
        self.dataset = dataset
        self.data = data
        '''self.api = self.datasources['data_sources']['api']
        print(self.api)
        self.csv = self.datasources['data_sources']['csv']
        print(self.csv)
        self.database = self.datasources['data_sources']['database']
        print(self.database)'''
        if (self.datasource == 'api') & (self.data == 'Pollution'):
            pollutiondata = self.getapidata(self.data)
            trans = Transform()
            trans.apiPollution(pollutiondata)
        elif (self.datasource == 'api') & (self.data == 'Economy'):
            economydata = self.getapidata(self.data)
            trans = Transform()
            trans.apiEconomy(economydata)
        elif self.datasource == 'csv':
            cryptodata = self.getCSVdata(self.data)
            trans = Transform()
            trans.csvCryptoMarkets(cryptodata)
        elif (self.datasource == 'database') & (self.data == 'MySql'):
            mysqldata = self.getDatabase(self.data)
            trans = Transform()
            trans.databaseMySql(mysqldata)
        elif (self.datasource == 'database') & (self.data == 'SQLite3'):
            sqlitedata = self.getDatabase(self.data)
            trans = Transform()
            trans.databaseSQLite3(sqlitedata)
        else:
            print('No datasources')

    def getapidata(self, apiname):
        apiurl = self.dataset[apiname]
        print('url is: ' + apiurl)
        data = (urlopen(apiurl)).read()
        print('data is: ')
        print(data)
        text = json.loads(data)
        print(text)
        return text

    def getCSVdata(self, csvname):
        print(csvname)
        csvurl = self.dataset[csvname]
        print(csvurl)
        df = pd.read_csv(csvurl, nrows=3000)
        df.describe()
        print(df)
        df.to_csv("templates/crypto.csv", encoding='utf-8', index=False)
        return df

    def getDatabase(self, dbname):
        dbdetails = self.dataset[dbname]
        print(dbdetails)
        if dbname == 'MySql':
            host = dbdetails['HOST']
            user = dbdetails['USER']
            password = dbdetails['PASSWORD']
            database = dbdetails['NAME']
            charset = dbdetails['CHARSET']
            try:
                uri = 'mysql+mysqldb://' + user + ':' + password + '@' + host + '/' + database + '?charset=' + charset
                engine = create_engine(uri)
                mysqldb = engine.connect()
                print('connection successful')
                return mysqldb
            except UnicodeDecodeError as u:
                print(u)
        elif dbname == 'SQLite3':
            path = dbdetails['PATH']
            print(path)
            uri = 'sqlite:///' + path
            engine = create_engine(uri)
            sqlitedb = engine.connect()
            print('connection sucessful')
            sql = 'select * from country'
            df = pd.read_sql(sql, con=sqlitedb)
            print(df)
            return df

#ext = Extract()
#ext.getDatabase('SQLite3')

