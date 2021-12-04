#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Exploring simplest arbs

@author: ThaFranklin & GuiLamacie

Created on 12/01/2021
"""
#%% 
# # import libs
import os
import sys
import pandas as pd
import requests
import time
from datetime import datetime
# from pudb import set_trace
# from pdb import set_trace

DEBUG = True
PROD = False   

# s_path = '..\\'
s_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(s_path)

pd.set_option('display.max_columns',100)
pd.set_option('display.max_rows',100)

pd.set_option('precision', 3)
pd.set_option('display.float_format', lambda x: '%.6f' % x)

# %% 
# # list markets we want to monitor in BASE/QUOTE format
c_base = 'DOT'
c_quote = 'USDT'

# taker fees in %
d_fee_bps = {
  'FTX': 0.06, 
  'Binance': 0.08,
  'Kraken': 0.26,
  'Kucoin': 0.1
}

df_hist_arbs = pd.DataFrame()
i_seq = 0


def show_current_prices(ex1, base1, quote1, ex2, base2, quote2):
  if ex1 == 'FTX':
    mkt1 = base1 +"/"+ quote1
    p_mkt1 = requests.get(f'https://ftx.com/api/markets/{mkt1}/orderbook?depth=5').json()
    p_mkt1 = p_mkt1['result']
    p_sell = float(p_mkt1['bids'][0][0]) * (1-d_fee_bps[ex1]/100)
    qty_sell = p_mkt1['bids'][0][1]
    p_buy = float(p_mkt1['asks'][0][0]) * (1+d_fee_bps[ex1]/100)
    qty_buy = p_mkt1['asks'][0][1]

  elif ex1 == 'Binance':
    mkt1 = base1 + quote1
    p_mkt1 = requests.get(f'https://api.binance.com/api/v3/depth?symbol={mkt1}&limit=5').json()
    # p_mkt1.pop('lastUpdateId')
    p_sell = float(p_mkt1['bids'][0][0]) * (1-d_fee_bps[ex1]/100)
    qty_sell = p_mkt1['bids'][0][1]
    p_buy = float(p_mkt1['asks'][0][0]) * (1+d_fee_bps[ex1]/100)
    qty_buy = p_mkt1['asks'][0][1]

  elif ex1 == 'Kucoin':
    mkt1 = base1 + '-' + quote1
    p_mkt1 = requests.get(f'https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol={mkt1}').json()
    p_mkt1 = p_mkt1['data']
    p_sell = float(p_mkt1['bids'][0][0]) * (1-d_fee_bps[ex1]/100)
    qty_sell = p_mkt1['bids'][0][1]
    p_buy = float(p_mkt1['asks'][0][0]) * (1+d_fee_bps[ex1]/100)
    qty_buy = p_mkt1['asks'][0][1]

  else:
    # print('asset market not known')
    p_sell = 0
    qty_sell = 0
    p_buy = 0
    qty_buy = 0
  
  if ex2 == 'FTX':
    mkt2 = base2 +"/"+ quote2
    p_mkt2 = requests.get(f'https://ftx.com/api/markets/{mkt2}/orderbook?depth=3').json()
    p_mkt2 = p_mkt2['result']
    p_sell2 = float(p_mkt2['bids'][0][0]) * (1-d_fee_bps[ex2]/100)
    qty_sell2 = p_mkt2['bids'][0][1]
    p_buy2 = float(p_mkt2['asks'][0][0]) * (1+d_fee_bps[ex2]/100)
    qty_buy2 = p_mkt2['asks'][0][1]

  elif ex2 == 'Binance':
    mkt2 = base2 + quote2
    p_mkt2 = requests.get(f'https://api.binance.com/api/v3/depth?symbol={mkt2}&limit=3').json()
    p_sell2 = float(p_mkt2['bids'][0][0]) * (1-d_fee_bps[ex2]/100)
    qty_sell2 = p_mkt2['bids'][0][1]
    p_buy2 = float(p_mkt2['asks'][0][0]) * (1+d_fee_bps[ex2]/100)
    qty_buy2 = p_mkt2['asks'][0][1]

  elif ex2 == 'Kucoin':
    mkt2 = base2 + '-' + quote2
    p_mkt2 = requests.get(f'https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol={mkt2}').json()
    p_mkt2 = p_mkt2['data']
    p_sell2 = float(p_mkt2['bids'][0][0]) * (1-d_fee_bps[ex2]/100)
    qty_sell2 = p_mkt2['bids'][0][1]
    p_buy2 = float(p_mkt2['asks'][0][0]) * (1+d_fee_bps[ex2]/100)
    qty_buy2 = p_mkt2['asks'][0][1]

  else:
    # print('asset market not known')
    p_sell2 = 0
    qty_sell2 = 0
    p_buy2 = 0
    qty_buy2 = 0

  # print('first market: ')
  # print(f'{bid_price} {bid_qty} {ask_price} {ask_qty}')
  # print('second market: ')
  # print(f'{bid_price2} {bid_qty2} {ask_price2} {ask_qty2}')

  return [p_sell, qty_sell, p_buy, qty_buy, p_sell2, qty_sell2, p_buy2, qty_buy2]  
  # changed to returning effective taking prices instead of book prices
  # [bid_price, bid_qty, ask_price, ask_qty, bid_price2, bid_qty2, ask_price2, ask_qty2]

# exchangeInfo brings additional information about tokens
bin_info = requests.get('https://api.binance.com/api/v3/exchangeInfo').json()
kuc_info = requests.get('https://api.kucoin.com/api/v1/symbols').json()

df_bin_info = pd.DataFrame(bin_info['symbols'])
df_kuc_info = pd.DataFrame(kuc_info['data'])

#%% 
# # will loop until user cancel
while True: 
  s_time= datetime.utcnow().isoformat(sep=' ', timespec='milliseconds')
  # get data from all markets in FTX
  ftx_markets = requests.get('https://ftx.com/api/markets').json()

  # get data from all markets in Binance
  bin_markets = requests.get('https://api.binance.com/api/v3/ticker/24hr').json()
  
  # get data from all markets in Kraken
  # krak = requests.get('https://api.kraken.com/0/public/AssetPairs').json()
  # nao tem bid ask nesse request & tem q especificar o PAR /Ticker?pair=XBTUSD,1INCHUSD'

  # get data from all markets in Kucoin
  kuc = requests.get('https://api.kucoin.com/api/v1/market/allTickers').json()

  # processing FTX
  df_ftx=pd.DataFrame.from_dict(ftx_markets['result'])
  df_ftx = df_ftx.loc[df_ftx['enabled'] == True]
  sol_ftx = df_ftx.loc[df_ftx['baseCurrency'] == c_base, ['baseCurrency', 'quoteCurrency', 'bid', 'ask']]
  q_ftx = sol_ftx.loc[sol_ftx['quoteCurrency'] != c_quote, 'quoteCurrency'].unique()
  # ADD quote markets
  q_list1 = df_ftx['baseCurrency'].isin(q_ftx) & (df_ftx['quoteCurrency']==c_quote)
  aux_ftx = df_ftx.loc[q_list1, ['baseCurrency', 'quoteCurrency', 'bid', 'ask']]
  # put all markets from same exchange together
  sol_ftx = pd.concat([sol_ftx, aux_ftx], ignore_index=True)
  sol_ftx.columns=['base', 'quote', 'bid', 'ask']
  sol_ftx['exchange'] = 'FTX'
  # BUY prices and SELL prices (+ fee)
  sol_ftx['BUY_f'] = sol_ftx['ask']*(1+d_fee_bps['FTX']/100)
  sol_ftx['SELL_f'] = sol_ftx['bid']*(1-d_fee_bps['FTX']/100)

  # processing BINANCE
  try:
    df_bin = pd.DataFrame.from_dict(bin_markets)
    df_bin = df_bin.merge(
      df_bin_info[['symbol', 'status', 'baseAsset', 'quoteAsset']],
      how='left', left_on='symbol', right_on='symbol'
    )
    df_bin = df_bin.loc[df_bin['status']=='TRADING']
  except ValueError:
    print(bin_markets)

  sol_bin = df_bin.loc[df_bin['baseAsset'] == c_base, ['baseAsset', 'quoteAsset', 'bidPrice', 'askPrice']]
  q_bin = sol_bin.loc[sol_bin['quoteAsset'] != c_quote, 'quoteAsset'].unique()
  # ADD quote markets
  q_list2 = df_bin['baseAsset'].isin(q_bin) & (df_bin['quoteAsset']==c_quote)
  aux_bin = df_bin.loc[q_list2, ['baseAsset', 'quoteAsset', 'bidPrice', 'askPrice']]
  # put all markets from same exchange together
  sol_bin = pd.concat([sol_bin, aux_bin], ignore_index=True)
  sol_bin.columns=['base', 'quote', 'bid', 'ask']
  sol_bin['exchange'] = 'Binance'
  # BUY prices and SELL prices (+ fee)
  col_to_cast = ['bid', 'ask']
  sol_bin[col_to_cast] = sol_bin[col_to_cast].astype(dtype='float32')
  sol_bin['BUY_f'] = sol_bin['ask']*(1+d_fee_bps['Binance']/100)
  sol_bin['SELL_f'] = sol_bin['bid']*(1-d_fee_bps['Binance']/100)

  # processing KRAKEN
  # df_krak = pd.DataFrame(krak['result']).T.reset_index(drop=True)
  
  # processing KUCOIN
  df_kuc = pd.DataFrame(kuc['data']['ticker'])
  df_kuc = df_kuc.merge(
    df_kuc_info[['symbol','baseCurrency', 'quoteCurrency', 'enableTrading']],
    how='left', left_on='symbol', right_on='symbol'
  )
  df_kuc = df_kuc.loc[df_kuc['enableTrading'] == True]
  sol_kuc = df_kuc.loc[df_kuc['baseCurrency'] == c_base, ['baseCurrency', 'quoteCurrency', 'buy', 'sell']]
  q_kuc = sol_kuc.loc[sol_kuc['quoteCurrency'] != c_quote, 'quoteCurrency'].unique()
  # ADD quote markets
  q_list2 = df_kuc['baseCurrency'].isin(q_kuc) & (df_kuc['quoteCurrency']==c_quote)
  aux_kuc = df_kuc.loc[q_list2, ['baseCurrency', 'quoteCurrency', 'buy', 'sell']]
  # put all markets from same exchange together
  sol_kuc = pd.concat([sol_kuc, aux_kuc], ignore_index=True)
  sol_kuc.columns=['base', 'quote', 'bid', 'ask']
  sol_kuc['exchange'] = 'Kucoin'
  # BUY prices and SELL prices (+ fee)
  col_to_cast = ['bid', 'ask']
  sol_kuc[col_to_cast] = sol_kuc[col_to_cast].astype(dtype='float32')
  sol_kuc['BUY_f'] = sol_kuc['ask']*(1+d_fee_bps['Kucoin']/100)
  sol_kuc['SELL_f'] = sol_kuc['bid']*(1-d_fee_bps['Kucoin']/100)

  df_exchanges = pd.concat([sol_bin, sol_ftx, sol_kuc], ignore_index=True)

  # # add triangular "virtual" markets
  l_t = df_exchanges.loc[df_exchanges['quote'] != c_quote, 'quote'].to_list()
  cols_t = ['base', 'quote', 'exchange', 'BUY_f', 'SELL_f']
  mask_t = df_exchanges['base'].isin(l_t) & (df_exchanges['quote'] == c_quote)

  df_exchanges = df_exchanges.merge(
    df_exchanges.loc[mask_t, cols_t],
    how='left',
    left_on= 'quote',
    right_on= 'base',
    suffixes=('', '_t')
  )

  df_exchanges['BUY'] = (df_exchanges['BUY_f_t'] * df_exchanges['BUY_f']).fillna(df_exchanges['BUY_f'])
  df_exchanges['SELL'] = (df_exchanges['SELL_f_t'] * df_exchanges['SELL_f']).fillna(df_exchanges['SELL_f'])

  # select only markets starting with SOL and quoted in USDT
  arb_markets = df_exchanges.loc[(df_exchanges['base']==c_base)&((df_exchanges['quote']==c_quote)|(df_exchanges['quote_t']==c_quote))]

  #%% TO-DO
  # # Quantity

  # # Slippage discount

  # # triger arb (execute orders AND/OR keep for statistics)
  if arb_markets['BUY'].min() < arb_markets['SELL'].max():
    i_seq += 1

    l_cols = ['base', 'quote', 'exchange', 'base_t', 'quote_t', 'exchange_t', 'BUY','SELL']
    l_book = ['p_sell', 'qty_sell', 'p_buy', 'qty_buy', 'p_sell_t', 'qty_sell_t', 'p_buy_t', 'qty_buy_t']  
    # l_book = ['bid', 'bid_qty', 'ask', 'ask_qty', 'bid_t', 'bid_qty_t', 'ask_t', 'ask_qty_t']

    opp_buy = arb_markets[l_cols].sort_values('BUY', ascending=True).head(1).squeeze()     #.drop('SELL')
    opp_sell = arb_markets[l_cols].sort_values('SELL', ascending=False).head(1).squeeze()  #.drop('BUY')
    
    s_time_book = datetime.utcnow().isoformat(sep=' ', timespec='milliseconds')
    buy_book = show_current_prices(opp_buy['exchange'], opp_buy['base'], opp_buy['quote'], opp_buy['exchange_t'], opp_buy['base_t'], opp_buy['quote_t'])        
    sell_book = show_current_prices(opp_sell['exchange'], opp_sell['base'], opp_sell['quote'], opp_sell['exchange_t'], opp_sell['base_t'], opp_sell['quote_t'])
    
    opp_buy['id'] = i_seq
    opp_sell['id'] = i_seq
    opp_buy['s_time'] = s_time
    opp_sell['s_time'] = s_time
    opp_buy['s_time_book'] = s_time_book
    opp_sell['s_time_book'] = s_time_book

    for col in range(len(l_book)):
      opp_buy[l_book[col]] = buy_book[col]
      opp_sell[l_book[col]] = sell_book[col]
  
    df_hist_arbs = df_hist_arbs.append([opp_buy, opp_sell],ignore_index=True)
    
    print('new arb opportunity: ', flush=True)
    print(df_hist_arbs.tail(2), flush=True)

    # print(f'current buy prices: {buy_prices}')
    # print(f'current sell prices: {sell_prices}')

  # else:
    # print(f'[{s_time}] No arbs opportunities found')

  if df_hist_arbs.index.max() > 100:
    df_hist_arbs.to_csv(os.path.join(s_path,'df_hist_arbs_3_cex.csv'), mode='a')
    df_hist_arbs = pd.DataFrame()
  time.sleep(10)

  

# %%
