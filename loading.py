import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from urllib import parse


class Loading:

    def __init__(self, **kwargs):
        print(kwargs)
        self.database = kwargs['database']
        print(self.database)
#        r"C:\Users\prapu\Downloads\SQLite\db.sqlite3":
#        if kwargs['database'] == 'SQLite3':
        pollutiontable = '''CREATE TABLE IF NOT EXISTS POLLUTION(
                                            id integer PRIMARY KEY,
                                            location text NOT NULL,
                                            city text NOT NULL,
                                            country text NOT NULL,
                                            parameter text NOT NULL,
                                            value text NOT NULL
                                            );'''
        economytable = '''CREATE TABLE IF NOT EXISTS INDIAECONOMY(
                                            financialyear text NOT NULL,
                                            thisyeargdp integer NOT NULL,
                                            growthrate real
                                            );'''
        countrytable = '''CREATE TABLE IF NOT EXISTS COUNTRY(
                                           countrycode text NOT NULL,
                                           countryname text,
                                           country_region text NOT NULL,
                                           city_name text NOT NULL,
                                           city_population integer,
                                           language text
                                           );'''
        if kwargs['database'] == 'SQLite3':
            self.conn = self.connectdatabase()
            print('database connection successful')
            if self.conn is not None:
                self.createtable(self.conn, pollutiontable)
                self.createtable(self.conn, economytable)
                self.createtable(self.conn, countrytable)
            else:
                print('database not connected')
        elif kwargs['database'] == 'MySql':
            print(kwargs['dbname'])
            dbdetails = kwargs['dbname']
            host = dbdetails['HOST']
            user = dbdetails['USER']
            password = dbdetails['PASSWORD']
            database = dbdetails['NAME']
            charset = dbdetails['CHARSET']
            mysqldb = None
            try:
                uri = 'mysql+mysqldb://' + parse.quote_plus(user) + ':'\
                      + parse.quote_plus(password) + '@' + host + '/' \
                      + database + '?charset=' + charset
                engine = create_engine(uri)
                mysqldb = engine.connect()
                print('connection successful')

                df = pd.read_sql("SELECT * FROM city", con=mysqldb)
                print(df)

            except UnicodeDecodeError as u:
                print(u)
                mysqldb.close()

    def connectdatabase(self):
        mysqlite3 = r"C:\Users\prapu\Downloads\SQLite\db.sqlite3"
        conn = None
        '''uri = 'sqlite:///' + mysqlite3
        engine = create_engine(uri)
        sqlitedb = engine.connect()
        print('connection sucessful')'''
        try:
            conn = sqlite3.connect(mysqlite3)
            print(conn)
            return conn
        except ConnectionError as e:
            print('connection unsuccesful')
            print(e)
        return conn

    def createtable(self, sqliteconn, table):
        try:
            c = sqliteconn.cursor()
            c.execute(table)
            print('table created')
        except ConnectionError as e:
            print(e)

    def insertdf(self, collection, tablename):
        print('table is ' + tablename)
        print(type(collection))
        if isinstance(collection, pd.DataFrame):
            print('collection is dataframe')
            try:
                collection.to_sql(tablename, self.conn, if_exists='append',
                                  index=False)
                print('data inserted successfully')
            except ConnectionError as e:
                print(e)

    def updatedf(self, dataframe, tablename):
        dataframe.to_sql(tablename, self.conn, if_exists='replace',
                         index=False)
        print('updated')
