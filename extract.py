import yfinance as yf
import pandas as pd
from datetime import date
import requests

tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN','AMD','INTC']
raw_dfs= {}
today = date.today()
today = today.strftime('%Y-%m-%d')

for ticker in tickers:
    tk=yf.Ticker(ticker)
    raw_df= pd.DataFrame(tk.history(period='1d'))
    raw_df.columns=raw_df.columns.str.lower()
    raw_df=raw_df[['open', 'high', 'low', 'close']]

    raw_dfs[ticker]=raw_df


response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
btc_raw = response.json()
btc_raw = float(btc_raw['data']['amount'])

btc_index = pd.to_datetime([today])
btc_dct={'btc_usd':btc_raw}
btc_raw = pd.DataFrame(btc_dct, index=btc_index)

raw_dfs['btc_usd']=btc_raw

print(raw_dfs)

