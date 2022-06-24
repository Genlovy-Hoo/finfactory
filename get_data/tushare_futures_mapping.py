# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import dramkit.datetimetools as dttools
from dramkit import isnull, logger_show
from dramkit.other.othertools import archive_data
from dramkit.datetimetools import get_recent_workday_chncal
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import parms_check_ts_daily
from finfactory.utils.utils import get_tushare_api


COLS_FINAL = ['code', 'date', 'mapping_code']


def check_loss(data, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('主力与连续合约映射数据有缺失日期：'+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def get_future_mapping(code, date=None, ts_api=None):
    '''tushare获取主力与连续合约映射数据，指定主力/连续合约代码和日期'''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(date):
        date = get_recent_workday_chncal(dirt='pre')
    date = dttools.date_reformat(date, '')
    df = ts_api.fut_mapping(ts_code=code, trade_date=date)
    return df['mapping_ts_code'].iloc[0]
        
        
def get_futures_mapping(start_date='20220601',
                        end_date=None,
                        ts_api=None):
    '''
    tushare获取主力与连续合约映射数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    df = ts_api.fut_mapping(start_date=start_date, end_date=end_date)
    df.rename(columns={'ts_code': 'code',
                       'trade_date': 'date',
                       'mapping_ts_code': 'mapping_code'},
              inplace=True)
    df['date'] = df['date'].apply(
                 lambda x: dttools.date_reformat(x, '-'))
    return df[COLS_FINAL]


def update_futures_mapping(df_exist=None, fpath=None,
                           start_date='19950416', end_date=None,
                           ts_api=None, logger=None):
    '''增量更新主力与连续合约映射数据'''
    
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
        logger_show('主力与连续合约映射最新数据已存在，不更新。', logger)
        return df_exist
    
    dates = dttools.get_dates_between(start_date, end_date,
                                      keep1=True, keep2=True)
    dates.sort()
    if len(dates) == 0:
        logger_show('主力与连续合约映射最新数据已存在，不更新。', logger)
        return df_exist
    
    logger_show('更新主力与连续合约映射数据, {}->{} ...'.format(dates[0], dates[-1]),
                logger, 'info')
    data = []
    nget = 0
    for date1, date2 in dttools.cut_date(start_date, end_date, 4):
        logger_show('{}->{}...'.format(date1, date2), logger)
        df = get_futures_mapping(date1, date2, ts_api)
        data.append(df)
        nget += 1
        if nget % 100 == 0:
            # 防止报错丢失，在迭代过程中保存数据
            data_ = pd.concat(data, axis=0)[COLS_FINAL]
            if data_.shape[0] > 0:
                _ = archive_data(data_, df_exist,
                                 sort_cols=['date', 'code'],
                                 del_dup_cols=['date', 'code'],
                                 sort_first=False,
                                 csv_path=fpath,
                                 csv_index=None)
            logger_show('pausing...', logger)
            time.sleep(65)
    data = pd.concat(data, axis=0)
    if data.shape[0] == 0:
        logger_show('主力与连续合约映射数据获取0条记录，返回已存在数据。',
                    logger, 'warn')
        return df_exist
    
    # 统一字段名    
    data = data[COLS_FINAL].copy()
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols=['date', 'code'],
                            del_dup_cols=['date', 'code'],
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None)
    
    return data_all


def update_futures_mapping_check(save_path=None,
                                 root_dir=None,
                                 start_date='19950416',
                                 trade_dates=None,
                                 ts_api=None,
                                 logger=None):
    '''
    更新主力与连续合约映射数据
    '''
    
    def _get_save_path(save_path):
        '''获取主力与连续合约映射数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('futures/tushare/mapping/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'future_mapping.csv'
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)
    data_all = update_futures_mapping(df_exist=None,
                                      fpath=save_path,
                                      start_date=start_date,
                                      end_date=None,
                                      ts_api=ts_api,
                                      logger=logger)
    
    loss_dates = check_loss(data_all, trade_dates, logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_futures_mapping_check(*args, **kwargs):
        return update_futures_mapping_check(*args, **kwargs)
    
    
    trade_dates_sh = load_trade_dates_tushare('SSE')
    trade_dates_sz = load_trade_dates_tushare('SZSE')    
    
    df, loss_dates = try_update_futures_mapping_check(
                                        save_path=None,
                                        root_dir=None,
                                        start_date='19950416',
                                        trade_dates=trade_dates_sh,
                                        ts_api=ts_api,
                                        logger=logger)
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
