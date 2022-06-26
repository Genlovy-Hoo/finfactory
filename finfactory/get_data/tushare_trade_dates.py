# -*- coding: utf-8 -*-

import os
import numpy as np
import dramkit.datetimetools as dttools
from dramkit import isnull, logger_show
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import parms_check_ts_daily
from finfactory.utils.utils import get_tushare_api


COLS_FINAL = ['exchange', 'date', 'is_open', 'pre_trade_date']


def check_loss(data, exchange, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 only_workday=False,
                                 del_weekend=False)
    if len(loss_dates) > 0:
        logger_show('{}交易日数据有缺失日期：'.format(exchange)+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates
        
        
def get_his_trade_dates(exchange='SSE',
                        start_date='19891231',
                        end_date=None,
                        ts_api=None):
    '''
    | tushare获取历史交易日数据
    | start_date和end_date格式：'20220611'
    | exchange表示交易所（与tushare官方文档一致，默认上交所）:
    |     SSE上交所, SZSE深交所, CFFEX中金所, SHFE上期所,
    |     CZCE郑商所, DCE大商所, INE上能源
    | exchange若为None，则默认上交所
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(exchange):
        exchange = 'SSE'
    if isnull(end_date):
        end_date = dttools.today_date('')
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    df = ts_api.trade_cal(exchange=exchange,
                          start_date=start_date, end_date=end_date)
    df.rename(columns={'cal_date': 'date',
                       'pretrade_date': 'pre_trade_date'},
              inplace=True)
    df['date'] = df['date'].apply(
                 lambda x: dttools.date_reformat(x, '-'))
    df['pre_trade_date'] = df['pre_trade_date'].apply(
                 lambda x: dttools.date_reformat(x, '-') if not isnull(x) else np.nan)
    return df[COLS_FINAL]


def update_trade_dates(exchange='SSE', df_exist=None, fpath=None,
                       start_date=None, end_date=None, ts_api=None,
                       logger=None):
    '''增量更新历史交易日数据'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if isnull(df_exist):
        if isnull(fpath) or (not os.path.exists(fpath)):
            lag = 0
        else:
            lag = 1
        last_date, start_date, end_date_, df_exist = \
            parms_check_ts_daily(fpath, default_last_date=start_date,
                                 start_lag=lag)
    else:
        last_date, start_date, end_date_, df_exist = \
            parms_check_ts_daily(df_exist, default_last_date=start_date,
                                 start_lag=1)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
    if isnull(end_date):
        end_date = end_date_
    
    if last_date >= end_date:
        logger_show('{}交易日历最新数据已存在，不更新。'.format(exchange),
                    logger, 'info')
        return df_exist
    
    logger_show('更新{}历史交易日期数据, {}->{}...'.format(
                exchange, start_date, end_date),
                logger, 'info')
    df = get_his_trade_dates(exchange, start_date, end_date, ts_api)
    
    if df.shape[0] == 0 or isnull(df):
        logger_show('{}新获取0条记录！'.format(exchange),
                    logger, 'warn')
        return df_exist
    
    # 统一字段名    
    df = df[COLS_FINAL].copy()
    # 数据合并
    data_all = archive_data(df, df_exist,
                            sort_cols='date',
                            del_dup_cols='date',
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None)
    
    return data_all


def update_trade_dates_check(exchange='SSE',
                             save_path=None,
                             root_dir=None,
                             start_date='19891231',
                             ts_api=None,
                             logger=None):
    '''
    更新交易日期数据
    '''
    
    def _get_save_path(save_path):
        '''获取交易日历史数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('trade_dates/tushare/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}.csv'.format(exchange)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)
    data_all =  update_trade_dates(exchange,
                                   df_exist=None,
                                   fpath=save_path,
                                   start_date=start_date,
                                   end_date=None,
                                   ts_api=ts_api,
                                   logger=logger) 
    
    loss_dates = check_loss(data_all, exchange, logger=logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_trade_dates_check(*args, **kwargs):
        return update_trade_dates_check(*args, **kwargs)
    
    
    exs = {
            'SSE': '1990-12-18', # 上交所
            'SZSE': '1991-07-02', # 深交所
            'CFFEX': '2006-09-07', # 中金所
            'SHFE': '1991-05-27', # 上期所
            'CZCE': '1990-10-11', # 郑商所
            'DCE': '1993-02-28', # 大商所
            'INE': '2017-05-22' # 上能源
        }
    for ex, start_date in exs.items():
        exec('''df_{}, loss_{} = try_update_trade_dates_check(
                                    ex,
                                    save_path=None,
                                    root_dir=None,
                                    start_date=start_date,
                                    ts_api=ts_api,
                                    logger=logger
                            )
             '''.format(ex, ex)
            )
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
