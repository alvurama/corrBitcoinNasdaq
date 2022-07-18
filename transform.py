from extract import raw_dfs, tickers, today
import numpy as np
import pandas as pd

raw_dfs = raw_dfs.copy()
tickers = tickers.copy()


# Ingeniería características
# A crear: dif_apert_cierre / rango día / signo_dia

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

print('*'*50)
print(tablon)
