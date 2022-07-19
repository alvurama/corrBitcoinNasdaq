import numpy as np
import prefect
import requests
import pandas as pd
import yfinance as yf
import pyodbc
from prefect import task, Flow
from datetime import date
from prefect.tasks.secrets import PrefectSecret


tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN', 'AMD', 'INTC']
today = date.today()
today = today.strftime('%Y-%m-%d')


@task
def extract(ticker, today):
    raw_dfs = {}
    for ticker in tickers:
        tk = yf.Ticker(ticker)
        raw_df = pd.DataFrame(tk.history(period='1d'))
        raw_df.columns = raw_df.columns.str.lower()
        raw_df = raw_df[['open', 'high', 'low', 'close']]

        raw_dfs[ticker] = raw_df

    response = requests.get(
        'https://api.coinbase.com/v2/prices/spot?currency=USD')
    btc_raw = response.json()
    btc_raw = float(btc_raw['data']['amount'])

    btc_index = pd.to_datetime([today])
    btc_dct = {'btc_usd': btc_raw}
    btc_raw = pd.DataFrame(btc_dct, index=btc_index)

    raw_dfs['btc_usd'] = btc_raw

    return raw_dfs


@task
def transform(raw_dfs, tickers, today):
    # Ingeniería características
    for ticker in tickers:
        df = raw_dfs[ticker]
        df['dif_apert_cierre'] = df['open']-df['close']
        df['rango_dia'] = df['high']-df['low']
        df['signo_dia'] = np.where(df['dif_apert_cierre'] > 0.0,
                                   '+', np.where(df['dif_apert_cierre'] < 0.0, '-', '0'))

        df = df[['close', 'dif_apert_cierre', 'rango_dia', 'signo_dia']]
        df.columns = map(lambda x: ticker + '_' + x, df.columns.to_list())

        raw_dfs[ticker] = df

    # Agregado
    dfs_list = raw_dfs.values()
    tablon = pd.concat(dfs_list, axis=1)

    return tablon


@task
def load(tablon, today,credentials):
    num_rows_tablon = len(tablon.index)

    if num_rows_tablon != 1:
        print('Error: Hay conflicto en la actualzación de los datos. Probablemente no se haya operado en NASDAQ el día de hoy.')
        return

    # 3.2
    nombre_cols_sql = ['[' + col + ']' for col in tablon.columns]
    sql_create_valores_btc = '''
        IF NOT EXISTS ( SELECT name FROM sys.tables WHERE name = 'btcvalores')
            CREATE TABLE btcvalores (
                [fecha] DATE PRIMARY KEY,
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} VARCHAR,
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} VARCHAR,
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} VARCHAR,
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} VARCHAR,
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} VARCHAR,
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} DECIMAL(20,2),
                {} VARCHAR,
                {} DECIMAL(20,2)
    
        )'''.format(*nombre_cols_sql)

    server = 'ALVARO\SQLSERVER19'
    database = 'caso_nasdaq_btc'
    user = 'sa'
    #password = '1218'  # secreto clave: pwd_sql

    cnxn = pyodbc.connect(
        driver='{SQL Server}', server=server, database=database, uid=user, pwd=credentials)

    cursor = cnxn.cursor()
    cursor.execute(sql_create_valores_btc)
    cnxn.commit()

    # 3.3
    sql_exists = "SELECT fecha FROM [dbo].[btcvalores] WHERE fecha = '{}'".format(
        str(today))
    cursor.execute(sql_exists)
    row = cursor.fetchone()
    if row:
        print('Error: Ya existe un registro para el día de hoy.')
        return

    # 3.4
    #tablon.insert(0, 'fecha', today)
    tablon['fecha'] = tablon.index

    for index, row in tablon.iterrows():
        cursor.execute('INSERT INTO dbo.btcvalores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )', row.tolist())

        cnxn.commit()
    cursor.close()
    cnxn.close()


with Flow("ETL Caso") as flow:
    tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN', 'AMD', 'INTC']
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    credentials= PrefectSecret('pwd_sql')
    raw_dfs = extract(tickers, today)
    tablon = transform(raw_dfs, tickers, today)
    load(tablon, today,credentials)

flow.run()
