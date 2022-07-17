# -*- coding: utf-8 -*-

import os
import sys
import time
from tqdm import tqdm
from dramkit import load_csv
from finfactory.load_his_data import find_target_dir


def cut_holder(dirs, ext):
    print(f'cut stocks {ext} ...\n')
    time.sleep(0.2)
    fdir = find_target_dir('stocks/tushare/stocks_holders/')
    save_path = f'{fdir}/astocks_{ext}.csv'
    data = load_csv(save_path, encoding='gbk')
    for c, d in tqdm(dirs.items(), file=sys.stdout):
        fpath = f'{d}/{c}_{ext}.csv'
        df = data[data['code'] == c]
        if df.shape[0] > 0:
            df.to_csv(fpath, encoding='gbk', index=None)
        else:
            tqdm.write(f'{c}无数据')


if __name__ == '__main__':
    strt_tm = time.time()
    
    
    # 单只股票数据存放路径
    fdir = find_target_dir('stocks/tushare/')
    codes = [x for x in os.listdir(fdir) if x not in \
                 ['stocks_daily', 'stocks_holders']]
    dirs = {x: fdir+x for x in codes if os.path.isdir(fdir+x)}
    
    # # top10holders
    # df_top10holders = cut_holder(dirs, 'top10holders')
    
    # # top10holders_free
    # df_top10holders_free = cut_holder(dirs, 'top10holders_free')
    
    # # zjc
    # df_zjc = cut_holder(dirs, 'zjc')
    
    
    print(f'\nused time: {round(time.time()-strt_tm, 6)}s.')
    