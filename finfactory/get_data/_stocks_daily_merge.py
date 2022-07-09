# -*- coding: utf-8 -*-

import os
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
    for c, d in tqdm(dirs.items()):
        fpath = f'{d}/{c}_{ext}.csv'
        if not os.path.exists(fpath):
            print(f'{c}_{ext}不存在！')
            continue
        df = load_csv(fpath)
        if 'stk' in ext:
            df = df[df['date'] >= year+'-01-01']
        data.append(df)
    data = pd.concat(data, axis=0)
    data.sort_values(['date', 'code'], ascending=True,
                     inplace=True)
    data.reset_index(drop=True, inplace=True)
    data.to_csv(f'{fdir}stocks_daily/astocks_{ext}.csv',
                index=None)
    return data


if __name__ == '__main__':
    from finfactory.load_his_data import find_target_dir
    
    strt_tm = time.time()
    
    
    # 单只股票行情存放路径
    fdir = find_target_dir('stocks/tushare/')
    codes = [x for x in os.listdir(fdir) if x != 'stocks_daily']
    dirs = {x: fdir+x for x in codes if os.path.isdir(fdir+x)}
    
    # # daily
    # df = merge_daily(dirs)
    
    # # daily_stk
    # df_stk = merge_daily(dirs, 'daily_stk')
    
    # # daily_basic
    # df_stk = merge_daily(dirs, 'daily_basic')
    
    
    print(f'\nused time: {round(time.time()-strt_tm, 6)}s.')
    