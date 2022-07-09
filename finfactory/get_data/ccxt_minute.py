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


def check_minute_loss(data, symbol, minute, logger=None):
    '''检查分钟数据缺失'''
    df_loss = check_loss(data, '{}min'.format(int(minute)))
    if df_loss.shape[0] > 0:
        logger_show('{}, {}minute数据有缺失！'.format(symbol, int(minute)),
                    logger, 'warn')
    else:
        logger_show('{}, {}minute数据无缺失！'.format(symbol, int(minute)),
                    logger, 'info')
    return df_loss


def update_minute(symbol, minute, df_exist=None,
                  fpath=None, start_time=None,
                  end_time=None, force=True,
                  mkt=None, logger=None):
    '''增量更新分钟数据'''
    assert (start_time is not None) or (not isnull(df_exist)), \
           '开始时间或存量数据必须存在一个'
    if df_exist is None:
        if not isnull(fpath) and os.path.exists(fpath):
            df = load_csv(fpath)
        else:
            df = get_klines_ccxt(symbol, start_time,
                                 freq='{}m'.format(int(minute)),
                                 mkt=mkt, logger=logger)
    else:
        df = df_exist.copy()
    df.sort_values('time', ascending=True, inplace=True)
    time_last = df['time'].max()
    if end_time is None:
        time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_now = False
    else:
        time_now = end_time
        seconds_dif = dttools.diff_time_second(time_now, time_last)
        if seconds_dif < 60*minute and not force:
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
        df_ = get_klines_ccxt(symbol,
                              start_time=time_last,
                              freq='{}m'.format(int(minute)),
                              mkt=mkt, logger=logger)
        df_.sort_values('time', ascending=True, inplace=True)
        df = pd.concat((df, df_), axis=0)
        df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        time_last = df['time'].max()
        # if end_time is None:
        #     time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        seconds_dif = dttools.diff_time_second(time_now, time_last)
        if seconds_dif < 60*minute:
            update_now = True
        # 在迭代中就保存数据防止报错时丢失已获取的数据
        df.sort_values('time', ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        if not isnull(fpath):
            if os.path.exists(fpath):
                shutil.copy(fpath, fpath[:-4]+'_bk'+fpath[-4:])
            df.to_csv(fpath, index=None)
    df.reset_index(drop=True, inplace=True)
    return df


def update_minute_check(symbol, minute,
                        save_path=None, root_dir=None,
                        name1=None, name2=None,
                        start_time='2022-06-01 00:00:00',
                        mkt='binance', logger=None):
    '''
    更新分钟行情数据
    '''
    
    def _get_save_path(save_path):
        '''获取分钟数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('{}/ccxt_{}/'.format(name1, mkt),
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}_{}minute.csv'.format(name2, int(minute))
        return save_path
    
    if isnull(mkt):
        mkt = 'binance'
    ccxt_mkt = get_ccxt_market(mkt)
    
    if isnull(save_path) and (isnull(name1) or isnull(name2)):
        raise ValueError('`save_path`和`name`至少指定一个！')
    
    save_path = _get_save_path(save_path)    
    data_all =  update_minute(symbol,
                              minute,
                              df_exist=None,
                              fpath=save_path,
                              start_time=start_time,
                              end_time=None,
                              force=True,
                              mkt=ccxt_mkt,
                              logger=logger)
    
    df_loss = check_minute_loss(data_all, symbol, minute, logger)
    
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
    def try_update_minute_check(*args, **kwargs):
        return update_minute_check(*args, **kwargs)
    
    
    # minutes = [30, 15, 5]
    # minutes = [1]
    minutes = [30, 15, 5, 1]
    symbols = [
        ('ETH/USDT', 'eth', 'eth_usdt',
         'binance', '2017-01-01 00:00:00'),
        ('BTC/USDT', 'btc', 'btc_usdt',
         'binance', '2012-01-01 00:00:00')
        ]
    
    for minute in minutes:
        for symbol, name1, name2, mkt, start_time in symbols:
            exec('''df_{}{}, df_{}{}_loss = try_update_minute_check(
                                                symbol,
                                                minute,
                                                save_path=None,
                                                root_dir=None,
                                                name1=name1, name2=name2,
                                                start_time=start_time,
                                                mkt=mkt,
                                                logger=logger)
                 '''.format(name1, minute, name1, minute)
                 )
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









