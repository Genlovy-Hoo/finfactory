# -*- coding: utf-8 -*-

import os
import sys
import time
import pandas as pd
from tqdm import tqdm
from dramkit import load_csv
from dramkit.datetimetools import today_date


def merge_daily(dirs, ext='daily'):
    year = today_date()[:4]
    data = []
    print(f'merge stocks {ext} ...\n')
    time.sleep(0.2)
    for c, d in tqdm(dirs.items(), file=sys.stdout):
        fpath = f'{d}/{c}_{ext}.csv'
        if not os.path.exists(fpath):
            tqdm.write(f'{c}_{ext}不存在！')
            continue
        df = load_csv(fpath)
        # df = df[df['date'] >= year+'-01-01']
        data.append(df)
    data = pd.concat(data, axis=0)
    data.sort_values(['date', 'code'], ascending=True,
                     inplace=True)
    data.reset_index(drop=True, inplace=True)
    data.to_csv(f'{fdir}stocks_daily/astocks_{ext}.csv',
                index=None)
    return data


def merge_holder(dirs, ext, sort_cols):
    data = []
    print(f'merge stocks {ext} ...\n')
    time.sleep(0.2)
    for c, d in tqdm(dirs.items(), file=sys.stdout):
        fpath = f'{d}/{c}_{ext}.csv'
        if not os.path.exists(fpath):
            tqdm.write(f'{c}_{ext}不存在！')
            continue
        df = load_csv(fpath, encoding='gbk')
        data.append(df)
    data = pd.concat(data, axis=0)
    data.sort_values(sort_cols, ascending=True,
                     inplace=True)
    data.reset_index(drop=True, inplace=True)
    data.to_csv(f'{fdir}stocks_holders/astocks_{ext}.csv',
                index=None)
    return data


if __name__ == '__main__':
    from finfactory.load_his_data import find_target_dir
    
    strt_tm = time.time()
    
    
    # 单只股票数据存放路径
    fdir = find_target_dir('stocks/tushare/')
    codes = [x for x in os.listdir(fdir) if x not in \
                 ['stocks_daily', 'stocks_holders']]
    dirs = {x: fdir+x for x in codes if os.path.isdir(fdir+x)}
    
    # # daily
    # df = merge_daily(dirs)
    
    # # daily_basic
    # df_stk = merge_daily(dirs, 'daily_basic')
    
    # # daily_stk
    # df_stk = merge_daily(dirs, 'daily_stk')
    
    # # top10holders
    # df_top10holders = merge_holder(dirs, 'top10holders',
    #                       ['code', '报告期', 'holder_name'])
    
    # # top10holders_free
    # df_top10holders_free = merge_holder(dirs, 'top10holders_free',
    #                         ['code', '报告期', 'holder_name'])
    
    # # zjc
    # df_zjc = merge_holder(dirs, 'zjc',
    #                       ['code', '公告日期', 'holder_name'])
    
    
    print(f'\nused time: {round(time.time()-strt_tm, 6)}s.')
    