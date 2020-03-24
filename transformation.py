#from extraction import Extract
import pandas as pd
from loading import Loading


class Transform:

    def apiPollution(self, poll_data):
        print(poll_data.keys())
        apidata = poll_data['results']
        print(apidata)

        pollgen = [data for data in apidata]
        print(pollgen)
        pollution = [[data['location'], data['city'], data['country'], measurement['parameter'], measurement['value'],
                      ] for data in apidata for measurement in data['measurements']]
        print(pollution)
        df = pd.DataFrame(pollution, columns=['Location', 'City', 'Country', 'Parameter', 'Value'])
        print(df)
        print(df.head())

        load = Loading(database='SQLite3')
        print('Pollution connection')
        load.insertdf(df, 'POLLUTION')
        print('data loadede successfully')

    def apiEconomy(self, economy_data):
        print(economy_data.keys())
        print(economy_data['records'])
        f = economy_data['records']
        df = pd.DataFrame(f)
        print(df)
        gdp = {record['financial_year']: int(record['gross_domestic_product_in_rs_cr_at_2004_05_prices']) for record in
               f}
        print(len(gdp))
        lgdp = list(gdp)
        print(lgdp)
        print(len(lgdp))

        annualgdp = []
        for i in range(len(lgdp)):
            gdpindia = {}
            if i == 0:
                gdpindia['financialyear'] = lgdp[i]
                gdpindia['thisyeargdp'] = gdp[lgdp[i]]
                gdpindia['growthrate'] = None

            else:
                print(lgdp[i])
                gdpindia['financialyear'] = lgdp[i]
                gdpindia['thisyeargdp'] = gdp[lgdp[i]]
                gdpindia['growthrate'] = round((((gdp[lgdp[i]] - gdp[lgdp[i-1]]) / gdp[lgdp[i-1]]) * 100), 2)
            print(gdpindia)
            annualgdp.append(gdpindia)
            print(annualgdp)

        df = pd.DataFrame(annualgdp)
        print(df)

        load = Loading(database='SQLite3')
        print('Economy connection')
        load.insertdf(df, 'INDIAECONOMY')
        print('data loaded successfully')

    def csvCryptoMarkets(self, crypto_data):
        asset_code = ['BTC', 'ETH', 'XRP']
        print('crypto is')
        print(crypto_data['open'])
        crypto_data['open'] = crypto_data[['open', 'symbol']].apply(lambda x: round((float(x[0]) * 0.75), 3) if x[1] in
                                asset_code else pd.nan, axis=1)
        print(crypto_data['open'])
        crypto_data['close'] = crypto_data[['close', 'symbol']].apply(lambda x: round((float(x[0]) * 0.75), 3) if x[1] in
                                 asset_code else pd.nan, axis=1)
        crypto_data['high'] = crypto_data[['high', 'symbol']].apply(lambda x: round((float(x[0]) * 0.75), 3) if x[1] in
                                asset_code else pd.nan, axis=1)
        crypto_data['low'] = crypto_data[['low', 'symbol']].apply(lambda x: round((float(x[0]) * 0.75), 3) if x[1] in
                               asset_code else pd.nan, axis=1)
        crypto_data.dropna(inplace=True)
        crypto_data.to_csv("templates/cryptogbp.csv")

    def databaseMySql(self, mysql_data):
        print('MySql connection is')
        print(mysql_data)
        mysqlconn = mysql_data
        sql = "select country.code as countrycode, country.name as countryname, country.region as country_region, " \
              "city.name as city_name, city.population as city_population, countrylanguage.language" \
              " from country inner join city on city.countrycode = country.code inner join countrylanguage on " \
              "countrylanguage.countrycode = country.code where country.continent = 'asia';"
        df = pd.read_sql(sql, con=mysqlconn)
        print(df)
        mysqlconn.close()
        load = Loading(database='SQLite3')
        load.insertdf(df, 'COUNTRY')

    def databaseSQLite3(self, sqlite_data):
        print("SQLite3 connection is")
        print(sqlite_data['city_population'])
        sqlite_data['city_population'] = (sqlite_data[['city_population']]).apply((lambda x: float(x[0]) * 0.9), axis=1)
        print(sqlite_data['city_population'])
        print(sqlite_data)
        df = sqlite_data
        load = Loading(database='SQLite3')
        load.updatedf(df, 'COUNTRY')


#trans = Transform('database', 'SQLite3')
