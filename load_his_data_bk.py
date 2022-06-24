# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
from dramkit.gentools import isnull
from dramkit.iotools import load_csv
from dramkit.datetimetools import today_date
from dramkit.datetimetools import get_date_format
from dramkit.datetimetools import date_reformat
from dramkit.datetimetools import get_recent_workday_chncal
from dramkit.datetimetools import get_recent_inweekday_chncal
from dramkit.fintools.utils_chn import get_recent_trade_date_chncal

#%%
def get_15s_dir():
    '''查找15秒K线数据存放路径'''
    dirs = ['D:/Genlovy_Hoo/ProjectsSL/data/quotes/cf/',
            'E:/Genlovy_Hoo/HooProjects/ProjectsSL/data/quotes/cf/',
            'F:/Genlovy_Hoo/HooProjects/ProjectsSL/data/quotes/cf/',
            'G:/Genlovy_Hoo/HooProjects/ProjectsSL/data/quotes/cf/',
            'C:/Genlovy_Hoo/HooProjects/ProjectsSL/data/quotes/cf/',
            '/media/glhyy/DATA/Genlovy_Hoo/HooProjects/ProjectsSL/data/quotes/cf/']
    for dr in dirs:
        if os.path.exists(dr):
            return dr
    raise ValueError('未找到数据文件夹路径，请设置15秒K线数据路径！')


def get_1min_dir1():
    '''查找1分钟K线数据存放路径'''
    dirs = ['D:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/fund_minute/',
            'E:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/fund_minute/',
            'F:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/fund_minute/',
            'G:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/fund_minute/',
            'C:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/fund_minute/',
            '/media/glhyy/DATA/Genlovy_Hoo/HooProjects/FinFactory/data/archives/fund_minute/']
    for dr in dirs:
        if os.path.exists(dr):
            return dr
    raise ValueError('未找到数据文件夹路径，请设置1分钟K线数据路径！')


def get_1min_dir2():
    '''查找1分钟K线数据存放路径'''
    dirs = ['D:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_minute/qfq/',
            'E:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_minute/qfq/',
            'F:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_minute/qfq/',
            'G:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_minute/qfq/',
            'C:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_minute/qfq/',
            '/media/glhyy/DATA/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_minute/qfq/']
    for dr in dirs:
        if os.path.exists(dr):
            return dr
    raise ValueError('未找到数据文件夹路径，请设置1分钟K线数据路径！')


def get_daily_dir1():
    '''查找日K线数据存放路径'''
    dirs = ['D:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_daily/qfq/',
            'E:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_daily/qfq/',
            'F:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_daily/qfq/',
            'G:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_daily/qfq/',
            'C:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_daily/qfq/',
            '/media/glhyy/DATA/Genlovy_Hoo/HooProjects/FinFactory/data/archives/stocks_daily/qfq/']
    for dr in dirs:
        if os.path.exists(dr):
            return dr
    raise ValueError('未找到数据文件夹路径，请设置1分钟K线数据路径！')

#%%
def load_15s_data(code, start_date=None, end_date=None):
    '''读取15s数据'''

    data_dir = get_15s_dir()

    files = [x for x in os.listdir(data_dir) if code in x and '15s' in x]
    if start_date:
        files = [x for x in files if x[7:15] >= start_date]
    if end_date:
        files = [x for x in files if x[7:15] <= end_date]

    data = []
    for file in files:
        fpath = data_dir + file
        df = load_csv(fpath)
        data.append(df)

    data = pd.concat(data, axis=0)
    # data['date'] = data['date'].apply(lambda x: date_reformat(str(x)))
    data['date'] = data['date'].astype(str)
    data['time'] = data['date'] + ' ' + data['time']

    return data

#%%
def load_minute_data(code, start_date=None, end_date=None):
    '''读取分钟数据'''

    if code in ['510050', '510030', '159919']:
        fpath = get_1min_dir1() + code + '.csv'

    else:
        fpath = get_1min_dir2() + code + '.csv'

    data = load_csv(fpath)
    data['date'] = data['time'].apply(lambda x: x[:10])
    data['minute'] = data['time'].apply(lambda x: x[11:])

    if not isnull(start_date):
        data = data[data['date'] >= start_date]
    if not isnull(end_date):
        data = data[data['date'] <= end_date]

    return data


def handle_minute_930(df_minute):
    '''处理9.30分开盘行情'''
    def get_start(df):
        date = df['date'].iloc[0]
        start = pd.DataFrame(df.iloc[0, :]).transpose()
        start.loc[start.index[0], 'time'] = date + ' 09:30:00'
        start.loc[start.index[0], 'minute'] = '09:30:00'
        start.loc[start.index[0], 'close'] = start.loc[start.index[0], 'open']
        start.loc[start.index[0], ['open', 'low', 'high', 'volume', 'money',
                                   'pre_close']] = np.nan
        df = pd.concat((start, df), axis=0)
        return df
    df_minute_new = df_minute.groupby('date', as_index=False).apply(
                                                    lambda x: get_start(x))
    return df_minute_new


def get_minute_data(code, start_date=None, end_date=None):
    '''分钟行情数据获取'''
    df_minute = load_minute_data(code, start_date, end_date)
    df_minute = handle_minute_930(df_minute)
    return df_minute.reindex(columns=['time', 'date', 'minute', 'open',
                'close', 'low', 'high', 'volume', 'money']).set_index('time')


def load_daily_data(code):
    if code[0] == '6':
        fpath = get_daily_dir1() + code + '.XSHG_qfq.csv'
    else:
        fpath = get_daily_dir1() + code + '.XSHE_qfq.csv'
    return load_csv(fpath)

#%%
def load_index_daily(code):
    '''
    读取指数日K线数据
    '''
    data_dir = find_target_dir('index_daily/')
    return load_csv(data_dir+code+'.csv')


def load_index_minute(code, freq_minute=1):
    '''
    读取指数分钟K线数据
    '''
    data_dir = find_target_dir('index_minute/')
    fpath = data_dir + code + '_' + str(int(freq_minute)) + 'm.csv'
    return load_csv(fpath)

#%%
def load_index_futures_daily(code):
    '''读取股指期货日K线数据'''
    data_dir = find_target_dir('index_futures_daily/')
    return load_csv(data_dir+code+'.csv')


def load_index_futures_minute(code, freq_minute=1):
    '''读取股指期货分钟K线数据'''
    data_dir = find_target_dir('index_futures_minute/')
    fpath = data_dir + code + '_' + str(int(freq_minute)) + 'm.csv'
    return load_csv(fpath)

#%%
if __name__ == '__main__':
    import time

    strt_tm = time.time()

    code = '688008'

    # 起止日期
    start_date, end_date = '2019-12-01', '2021-02-02'
    # 导入行情数据
    data = get_minute_data(code, start_date, end_date)

    # # 起止日期
    # start_date, end_date = '20200203', '20210129'
    # data = load_15s_data(code, start_date, end_date)
    # data.reset_index(drop=True, inplace=True)
    # data.set_index('time', inplace=True)

    df = load_daily_data('300033')


    print('used time: {}s.'.format(round(time.time()-strt_tm, 6)))