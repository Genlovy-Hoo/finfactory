# -*- coding: utf-8 -*-

import time
import numpy as np
import pandas as pd
from dramkit import plot_series
from dramkit.gentools import merge_df
from finfactory.load_his_data import load_daily_crypto_usdt
from finfactory.fintools.fintools import boll
from finfactory.finplot.plot_candle import plot_candle

#%%
if __name__ == '__main__':
    strt_tm = time.time()
    
    #%%
    data = load_daily_crypto_usdt('btc', 'btc_usdt')
    data.rename(columns={'volume': 'vol'}, inplace=True)
    
    df_boll = boll(data['close'], 20, 2)
    data = merge_df(data, df_boll, how='left', left_index=True,
                    right_index=True)
    
    plot_series(data, {'close': '.-k'}, grids=True, figsize=(9.5, 7))
    data_ = data[(data['time'] >= '2019-10-01') & \
                  (data['time'] <= '2021-12-31')]
    plot_candle(data_, args_ma=None, args_boll=[20, 2],
                # plot_below='vol',
                plot_below=None,
                figsize=(11, 9))
    data_ = data[(data['time'] >= '2017-03-01') & \
                  (data['time'] <= '2017-12-31')]
    plot_candle(data_, args_ma=None, args_boll=[20, 2],
                # plot_below='vol',
                plot_below=None,
                figsize=(11, 9))
    data_ = data[(data['time'] >= '2013-03-01') & \
                  (data['time'] <= '2013-12-30')]
    plot_candle(data_, args_ma=None, args_boll=[20, 2],
                # plot_below='vol',
                plot_below=None,
                figsize=(11, 9))
    data_ = data[(data['time'] >= '2013-03-01') & \
                  (data['time'] <= '2013-06-30')]
    plot_candle(data_, args_ma=None, args_boll=[20, 2],
                # plot_below='vol',
                plot_below=None,
                figsize=(11, 9))
    
    #%%
    data['year'] = data['time'].apply(lambda x: x[:4])
    data_year = []
    for year in list(data['year'].unique()):
        df = data[data['year'] == year]
        open_, close = df['open'].iloc[0], df['close'].iloc[-1]
        high, low = df['high'].max(), df['low'].min()
        vol = df['vol'].sum()
        data_year.append([year, open_, high, low, close, vol])
    data_year = pd.DataFrame(data_year, columns=['time', 'open', 'high',
                                                    'low', 'close', 'vol'])
    for col in ['open', 'high', 'low', 'close']:
        data_year[col] = data_year[col].apply(lambda x: np.log(x))
    data_year['year'] = data_year['time'].apply(lambda x: x[:4])
    plot_candle(data_year,
                args_ma=None,
                # args_boll=None,
                args_boll=[5, 2],
                plot_below='vol', figsize=(9.5, 9),
                fontsize_label=20)
    
    #%%
    data['month'] = data['time'].apply(lambda x: x[:7])
    data_month = []
    for month in list(data['month'].unique()):
        df = data[data['month'] == month]
        open_, close = df['open'].iloc[0], df['close'].iloc[-1]
        high, low = df['high'].max(), df['low'].min()
        vol = df['vol'].sum()
        data_month.append([month, open_, high, low, close, vol])
    data_month = pd.DataFrame(data_month, columns=['time', 'open', 'high',
                                                    'low', 'close', 'vol'])
    for col in ['open', 'high', 'low', 'close']:
        data_month[col] = data_month[col].apply(lambda x: np.log(x))
    data_month['year'] = data_month['time'].apply(lambda x: x[:4])
    # data_month = data_month[(data_month['year'] >= '2015') & \
    #                         (data_month['year'] <= '2018')]
    plot_candle(data_month,
                args_ma=None,
                # args_boll=None,
                args_boll=[20, 2],
                plot_below='vol', figsize=(9.5, 9),
                fontsize_label=20)
        
    #%%
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    