# -*- coding: utf-8 -*-

import pandas as pd
import ccxt
from dramkit import logger_show
from dramkit.datetimetools import str2timestamp
from dramkit.datetimetools import timestamp2str 


def get_ccxt_market(mkt='binance'):
    mkt = eval('ccxt.{}()'.format(mkt))
    return mkt
    
    
def check_loss(df, freq, tcol='time', return_loss_data=True):
    '''
    | 检查数字货币日频行情缺失情况
    | freq为频率，如'1d', '5min'等
    '''
    tmin = df[tcol].min()
    tmax = df[tcol].max()
    tall = pd.date_range(tmin, tmax, freq=freq)
    tcol_ = tcol+'_loss'
    tall = pd.DataFrame(tall, columns=[tcol_])
    tall[tcol_] = tall[tcol_].apply(
                  lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    df_all = pd.merge(tall, df, how='left',
                      left_on=tcol_, right_on=tcol)
    df_loss = df_all[df_all[tcol].isna()].copy()
    df_loss['date_loss'] = df_loss[tcol_].apply(lambda x: x[:10])
    df_loss = df_loss.reindex(columns=['date_loss', tcol_]+\
                              list(df.columns))
    if return_loss_data:
        return df_loss
    else:
        return df_all[tcol_].unique().tolist()
    
    
def get_klines_ccxt(symbol, start_time, freq='1d', n=None,
                    mkt=None, logger=None):
    '''
    | ccxt获取K线数据
    | freq如'1d', '1m'等
    '''
    if mkt is None:
        mkt = get_ccxt_market()
    since = int(str2timestamp(start_time) * 1000)
    logger_show(symbol+' '+start_time+' ...', logger, 'info')
    data = mkt.fetch_ohlcv(symbol, freq, since=since, limit=n)
    data = pd.DataFrame(data, columns=['time', 'open', 'high',
                                       'low', 'close', 'volume'])
    data['time'] = data['time'].apply(lambda x: timestamp2str(x))
    return data




