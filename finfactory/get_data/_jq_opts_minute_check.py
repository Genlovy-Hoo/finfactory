# -*- coding: utf-8 -*-

import os
import pandas as pd
from dramkit import load_csv, isnull


def get_opts_trade_time(opts_info_path):
    '''获取期权的开始交易日期和最后交易日期'''
    df = load_csv(opts_info_path, encoding='gbk')
    df = df[['code', '开始交易日期', '最后交易日期']]
    df.set_index('code', inplace=True)
    return df


def get_exist_trade_time(opts_mint_dir):
    '''获取每只期权分钟数据已经存在的最大时间'''
    files = os.listdir(opts_mint_dir)
    fpaths = [opts_mint_dir+f for f in files if f[-4:] == '.csv']
    exist_trade_times = []
    for fpath in fpaths:
        df = load_csv(fpath)
        df.dropna(subset=['open', 'close', 'low', 'high'],
                  how='all', inplace=True)
        max_time = df['time'].max()
        exist_trade_times.append([os.path.basename(fpath)[:-4], max_time])
    exist_trade_times = pd.DataFrame(exist_trade_times,
                                     columns=['code', '存档最新时间'])
    exist_trade_times.set_index('code', inplace=True)
    return exist_trade_times


def check_opts_exist_time(opts_info_path, opts_mint_dir):
    opts_trade_time = get_opts_trade_time(opts_info_path)
    exist_trade_times = get_exist_trade_time(opts_mint_dir)
    df = pd.merge(opts_trade_time, exist_trade_times, how='left', 
                  left_index=True, right_index=True)
    df = df[df['最后交易日期'] >= '2017-01-01'].copy()
    df['存档最新日期'] = df['存档最新时间'].apply(lambda x:
                                    x[:10] if not isnull(x) else '0000-00-00')
    df = df[df['存档最新日期'] < df['最后交易日期']]
    return df[['开始交易日期', '最后交易日期']]
    

if __name__ == '__main__':
    import time
    from finfactory.load_his_data import find_target_dir
    
    strt_tm = time.time()
    
    
    jq_dir = find_target_dir('options/joinquant/')
    jq_optsmin_dir = find_target_dir('options/joinquant/options_minute/')
    
    
    exs = ['SZSE', 'SSE']
    data = []
    for ex in exs:
        print(ex, '...')
        fpath = find_target_dir('options/tushare/options_info/')+ex+'.csv'
        df = check_opts_exist_time(fpath, jq_optsmin_dir)
        data.append(df)
    data = pd.concat(data, axis=0)
    data.to_csv(jq_dir+'opts_minute_to_update.csv')
        
    
    print('used: {}s.'.format(round(time.time()-strt_tm, 6)))
    
    
    
    
    
    
