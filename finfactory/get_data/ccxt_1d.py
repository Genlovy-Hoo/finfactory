# -*- coding: utf-8 -*-

import os
import shutil
import pandas as pd
from datetime import datetime
from dramkit import isnull
from dramkit import load_csv
from dramkit import logger_show
import dramkit.datetimetools as dttools
from finfactory.utils.utils_crypto import check_loss
from finfactory.utils.utils_crypto import get_ccxt_market
from finfactory.utils.utils_crypto import get_klines_ccxt


def check_daily_loss(data, symbol, logger=None):
    '''检查日线缺失'''
    df_loss = check_loss(data, '1d')
    if df_loss.shape[0] > 0:
        logger_show('{}日线数据有缺失！'.format(symbol),
                    logger, 'warn')
    else:
        logger_show('{}日线数据无缺失！'.format(symbol),
                    logger, 'info')
    return df_loss


def update_daily(symbol, df_exist=None, fpath=None,
                 start_time=None, end_time=None,
                 force=True, mkt=None, logger=None):
    '''
    | 增量更新日线数据
    | 注：日收盘时间为北京时间早上八点
    '''
    assert (start_time is not None) or (not isnull(df_exist)), \
           '开始时间或存量数据必须存在一个'
    if df_exist is None:
        if not isnull(fpath) and os.path.exists(fpath):
            df = load_csv(fpath)
        else:
            df = get_klines_ccxt(symbol, start_time, freq='1d',
                                 mkt=mkt, logger=logger)
    else:
        df = df_exist.copy()
    df.sort_values('time', ascending=True, inplace=True)
    date_last = df['time'].max()[:10]
    if end_time is None:
        date_now = datetime.now().strftime('%Y-%m-%d')
        update_now = False
    else:
        date_now = end_time
        if date_now >= date_last and not force:
            update_now = True
            df.sort_values('time', ascending=False, inplace=True)
            df.reset_index(drop=True, inplace=True)
            if not isnull(fpath):
                if os.path.exists(fpath):
                    shutil.copy(fpath, fpath[:-4]+'_bk'+fpath[-4:])
                df.to_csv(fpath, index=None)
        else:
            update_now = False
    while not update_now:
        df_ = get_klines_ccxt(symbol, start_time=date_last+' 08:00:00',
                              freq='1d', mkt=mkt, logger=logger)
        df_.sort_values('time', ascending=True, inplace=True)
        df = pd.concat((df, df_), axis=0)
        df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        date_last = df['time'].max()[:10]
        days_dif = dttools.diff_days_date(date_now, date_last)
        now_hour = datetime.now().hour
        if days_dif == 0 or (now_hour < 8 and days_dif == 1):
            update_now = True
        # 在迭代中就保存数据防止报错时丢失已获取的数据
        df.sort_values('time', ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        if not isnull(fpath):
            if os.path.exists(fpath):
                shutil.copy(fpath, fpath[:-4]+'_bk'+fpath[-4:])
            df.to_csv(fpath, index=None)
    return df


def update_daily_check(symbol, save_path=None, root_dir=None,
                       name1=None, name2=None,
                       start_time='2012-01-01 08:00:00',
                       mkt='binance', logger=None):
    '''
    更新日线行情数据
    '''
    
    def _get_save_path(save_path):
        '''获取日线数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('{}/ccxt_{}/'.format(name1, mkt),
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}_daily.csv'.format(name2)
        return save_path
    
    if isnull(mkt):
        mkt = 'binance'
    ccxt_mkt = get_ccxt_market(mkt)
    
    if isnull(save_path) and (isnull(name1) or isnull(name2)):
        raise ValueError('`save_path`和`name`至少指定一个！')
    
    save_path = _get_save_path(save_path)    
    data_all =  update_daily(symbol,
                             df_exist=None,
                             fpath=save_path,
                             start_time=start_time,
                             end_time=None,
                             force=True,
                             mkt=ccxt_mkt,
                             logger=logger)
    
    df_loss = check_daily_loss(data_all, symbol, logger)
    
    return data_all, df_loss


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()


    logger = gen_py_logger(sys.argv[0])

    
    @try_repeat_run(cfg.try_get_ccxt, logger=logger,
                    sleep_seconds=cfg.try_get_ccxt_sleep)
    def try_update_daily_check(*args, **kwargs):
        return update_daily_check(*args, **kwargs)
    
    
    symbols = [
        ('ETH/USDT', 'eth', 'eth_usdt',
         'binance', '2017-01-01 08:00:00'),
        ('BTC/USDT', 'btc', 'btc_usdt',
         'binance', '2012-01-01 08:00:00')
        ]
    
    for symbol, name1, name2, mkt, start_time in symbols:
        exec('''df_{}, df_{}_loss = try_update_daily_check(
                                        symbol,
                                        save_path=None,
                                        root_dir=None,
                                        name1=name1, name2=name2,
                                        start_time=start_time,
                                        mkt=mkt,
                                        logger=logger)
             '''.format(name1, name1)
             )
    

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









