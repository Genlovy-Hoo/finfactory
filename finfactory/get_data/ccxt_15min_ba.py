# -*- coding: utf-8 -*-

import os
import time
import datetime
import ccxt
import pandas as pd
import shutil
from dramkit import load_csv
from dramkit import logger_show
from dramkit.gentools import log_used_time
from dramkit.gentools import try_repeat_run
from dramkit.datetimetools import timestamp2str
from dramkit.datetimetools import str2timestamp
from dramkit.datetimetools import diff_time_second
from dramkit import simple_logger, close_log_file


# LOGGER = None
LOGGER = simple_logger('../../log/get_data_ccxt_15min_ba.log', 'a')
TRY_GET_DATA_TIMES = 2
TRY_GET_DATA_SLEEP = 5
MKT = ccxt.binance()


@try_repeat_run(TRY_GET_DATA_TIMES, logger=LOGGER, sleep_seconds=TRY_GET_DATA_SLEEP)
def get_15minute(symbol, start_time):
    since = int(str2timestamp(start_time) * 1000)
    logger_show(symbol + ' ' + start_time + ' ...', LOGGER, 'info')
    data = MKT.fetch_ohlcv(symbol, '15m', since=since)
    data = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    data['time'] = data['time'].apply(lambda x: timestamp2str(x))
    return data


@log_used_time(LOGGER)
def update_15minute(symbol, fpath, start_time=None, end_time=None):
    assert start_time is not None or os.path.exists(fpath), '开始时间或本地文件必须存在一个'
    if os.path.exists(fpath):
        df = load_csv(fpath)
    else:
        df = get_15minute(symbol, start_time=start_time)
    df.sort_values('time', ascending=True, inplace=True)
    time_last = df['time'].max()
    if end_time is None:
        time_now = timestamp2str(time.time(), '%Y-%m-%d %H:%M:%S')
        update_now = False
    else:
        time_now = end_time
        seconds_dif = diff_time_second(time_now, time_last)
        if seconds_dif < 60*15:
            update_now = True
            df.sort_values('time', ascending=False, inplace=True)
            df.reset_index(drop=True, inplace=True)
            if os.path.exists(fpath):
                shutil.copy(fpath, fpath[:-4]+'_bk'+fpath[-4:])
            df.to_csv(fpath, index=None)
        else:
            update_now = False
    while not update_now:
        df_ = get_15minute(symbol, start_time=time_last)
        df_.sort_values('time', ascending=True, inplace=True)
        df = pd.concat((df, df_), axis=0)
        df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        time_last = df['time'].max()
        # if end_time is None:
        #     time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        seconds_dif = diff_time_second(time_now, time_last)
        if seconds_dif < 60*15:
            update_now = True
        # 在迭代中就保存数据防止报错是丢失已获取的数据              
        df.sort_values('time', ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        if os.path.exists(fpath):
            shutil.copy(fpath, fpath[:-4]+'_bk'+fpath[-4:])
        df.to_csv(fpath, index=None)
    return df


def check_minute(df):
    # time_min = df['time'].min()
    # time_max = df['time'].max()
    # times_all = pd.date_range(time_min, time_max, freq='15min')
    # times_all = pd.DataFrame(times_all, columns=['time_all'])
    # times_all['time_all'] = times_all['time_all'].apply(
    #                         lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    # df_all = pd.merge(times_all, df, how='left',
    #                   left_on='time_all', right_on='time')
    # df_loss = df_all[df_all['time'].isna()].copy()
    # df_loss['date'] = df_loss['time_all'].apply(lambda x: x[:10])
    # return df_loss
    from finfactory.utils.utils_crypto import check_loss_crypto
    return check_loss_crypto(df, '15min')


if __name__ == '__main__':
    strt_tm = time.time()
    
    # ETH/USDT
    symbol = 'ETH/USDT'
    start_time = '2017-01-01 00:00:00'
    fpath = '../../data/archives/eth/binance/eth_15minute.csv'
    logger_show('update {} minute ...'.format(symbol), LOGGER, 'info')
    df_eth = update_15minute(symbol, fpath, start_time)
    eth_loss = check_minute(df_eth)
    if eth_loss.shape[0] == 0:
        logger_show('{}数据无缺失。'.format(symbol), LOGGER, 'info')
    else:
        logger_show('{}数据有缺失！'.format(symbol), LOGGER, 'warn') 


    # BTC/USDT
    symbol = 'BTC/USDT'
    start_time = '2012-01-01 00:00:00'
    fpath = '../../data/archives/btc/binance/btc_15minute.csv'
    logger_show('update {} minute ...'.format(symbol), LOGGER, 'info')
    df_btc = update_15minute(symbol, fpath, start_time)
    btc_loss = check_minute(df_btc)
    if btc_loss.shape[0] == 0:
        logger_show('{}数据无缺失。'.format(symbol), LOGGER, 'info')
    else:
        logger_show('{}数据有缺失！'.format(symbol), LOGGER, 'warn')
    
    close_log_file(LOGGER)
    
    
    print(f'used time: {round(time.time()-strt_tm)}s.')
    