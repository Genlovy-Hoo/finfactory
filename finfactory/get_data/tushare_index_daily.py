# -*- coding: utf-8 -*-

import os
import pandas as pd
from dramkit import isnull
from dramkit import logger_show
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.fintools.utils_chn import get_trade_dates
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import parms_check_ts_daily
from finfactory.utils.utils import get_tushare_api


COLS_FINAL = ['code', 'date', 'open', 'high', 'low', 'close',
              'pre_close', 'change', 'change_pct', 'volume',
              'amount']


def check_loss(data, code, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('{}日线数据有缺失日期：'.format(code)+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def get_index_daily(code, start_date, end_date=None, ts_api=None):
    '''
    | tushare获取指数日线数据
    | start_date和end_date格式：'20220610'
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    df = ts_api.index_daily(ts_code=code,
                            start_date=start_date,
                            end_date=end_date)
    df['vol'] = df['vol'] * 100 # tushare指数行情成交量单位为手
    df['amount'] = df['amount'] * 1000 # tushare指数行情成交额单位为千元
    cols = {'ts_code': 'code', 'trade_date': 'date',
            'pct_chg': 'change_pct', 'vol': 'volume'}
    df.rename(columns=cols, inplace=True)
    df['code'] = code
    df['date'] = df['date'].apply(lambda x: dttools.date_reformat(x, '-'))
    df = df[COLS_FINAL]
    return df


def update_index_daily(code, df_exist=None, fpath=None,
                       start_date='19901218', end_date=None,
                       trade_dates=None, ts_api=None, 
                       logger=None):
    '''增量更新指数日线数据'''
    
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
        logger_show('{}日线最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)    
    if len(dates) == 0:
        logger_show('{}日线最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        return df_exist
    
    logger_show('更新{}日线数据, {}->{} ...'.format(code, dates[0], dates[-1]),
                logger, 'info')
    data = []
    for date1, date2 in dttools.cut_date(start_date, end_date, 6000):
        df = get_index_daily(code, date1, date2, ts_api)
        data.append(df)
    data = pd.concat(data, axis=0)
    if data.shape[0] == 0:
        logger_show('{}新获取0条记录，返回已存在数据。'.format(code),
                    logger, 'warn')
        return df_exist
    
    # 统一字段名    
    data = data[COLS_FINAL].copy()
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols='date',
                            del_dup_cols='date',
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None)
    
    return data_all


def update_index_daily_check(code,
                             save_path=None,
                             root_dir=None,
                             start_date='19901218',
                             trade_dates=None,
                             ts_api=None,
                             logger=None):
    '''
    | 更新指数日线行情数据
    | code: tushare代码(带后缀，如000300.SH)
    | trade_dates: 历史交易（开市）日期数据或存档路径
    '''
    
    def _get_save_path(save_path):
        '''获取指数日线历史数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('index/tushare/{}/'.format(code),
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}_daily.csv'.format(code)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_index_daily(code,
                                  df_exist=None,
                                  fpath=save_path,
                                  start_date=start_date,
                                  end_date=None,
                                  trade_dates=trade_dates,
                                  ts_api=ts_api,
                                  logger=logger)
    
    loss_dates = check_loss(data_all, code, trade_dates, logger=logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    trade_dates_sh = load_trade_dates_tushare('SSE')
    trade_dates_sz = load_trade_dates_tushare('SZSE')
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_index_daily_check(*args, **kwargs):
        return update_index_daily_check(*args, **kwargs)
    
    
    # tushare代码
    codes = {
        '000001.SH': '1990-12-18', # '上证指数'
        '399006.SZ': '2010-05-31', # '创业板指'
        '399005.SZ': '2006-01-23', # '中小板指'
        '000016.SH': '2004-01-01', # '上证50'
        '000300.SH': '2005-01-03', # '沪深300'
        '399300.SZ': '2005-01-03', # '沪深300'
        '000905.SH': '2005-01-03', # '中证500'
        '000852.SH': '2005-01-03', # '中证1000'
        '000688.SH': '2019-12-30', # '科创50'
        '399001.SZ': '1991-04-02', # '深证成指'
    }
    keys = list(codes.keys())
    for k in range(len(keys)):
        code = keys[k]
        start_date = codes[code]
        if code.endswith('.SH'):
            trade_dates = trade_dates_sh
        elif code.endswith('.SZ'):
            trade_dates = trade_dates_sz
        exec('''df_{}, loss_{} = try_update_index_daily_check(
                                    code,
                                    save_path=None,
                                    root_dir=None,
                                    start_date=start_date,
                                    trade_dates=trade_dates,
                                    ts_api=ts_api,
                                    logger=logger)
             '''.format(code[:6], code[:6])
            )
        # if k % 5 == 0:
        #     time.sleep(61)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









