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


COLS_FINAL = ['code', 'date', '总市值', '流通市值', '总股本',
              '流通股本', '自由流通股本', '换手率', '换收益(自由流通)',
              'pe', 'pe_ttm', 'pb']


def check_loss(data, code, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('{}日基本数据有缺失日期：'.format(code)+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def get_index_daily_basic(code, start_date,
                          end_date=None, ts_api=None):
    '''
    | tushare获取指数日基本数据
    | start_date和end_date格式：'20220610'
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')    
    df = ts_api.index_dailybasic(
            ts_code=code,
            fields=('ts_code,trade_date,total_mv,float_mv,'
                    'total_share,float_share,free_share,'
                    'turnover_rate,turnover_rate_f,pe,pe_ttm,pb'),
            start_date=start_date,
            end_date=end_date)        
    df.rename(columns={
                'ts_code': 'code', 'trade_date': 'date',
                'total_mv': '总市值', 'float_mv': '流通市值',
                'total_share': '总股本', 'float_share': '流通股本',
                'free_share': '自由流通股本', 'turnover_rate': '换手率',
                'turnover_rate_f': '换收益(自由流通)'},
              inplace=True)
    df['code'] = code
    df['date'] = df['date'].apply(dttools.date_reformat)
    df = df[COLS_FINAL]
    return df


def update_index_daily_basic(code, df_exist=None, fpath=None,
                             start_date='20040101', end_date=None,
                             trade_dates=None, ts_api=None, 
                             logger=None):
    '''增量更新指数日线基本数据'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if isnull(df_exist):
        if isnull(fpath) or (not os.path.exists(fpath)):
            lag = 0
        else:
            lag = 1
        last_date, start_date, end_date_, df_exist = \
            parms_check_ts_daily(fpath, default_last_date=start_date,
                                 start_lag=lag, encoding='gbk')
    else:
        last_date, start_date, end_date_, df_exist = \
            parms_check_ts_daily(df_exist, default_last_date=start_date,
                                 start_lag=0)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
    if isnull(end_date):
        end_date = end_date_
    
    if last_date >= end_date:
        logger_show('{}日线基本数据最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)    
    if len(dates) == 0:
        logger_show('{}日线基本数据最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        return df_exist
    
    logger_show('更新{}日线基本数据, {}->{} ...'.format(code, dates[0], dates[-1]),
                logger, 'info')
    data = []
    for date1, date2 in dttools.cut_date(start_date, end_date, 2500):
        df = get_index_daily_basic(code, date1, date2, ts_api)
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
                            csv_index=None,
                            csv_encoding='gbk')
    
    return data_all


def update_index_daily_basic_check(code,
                                   save_path=None,
                                   root_dir=None,
                                   start_date='20040101',
                                   trade_dates=None,
                                   ts_api=None,
                                   logger=None):
    '''
    | 更新指数日线基本数据
    | code: tushare代码(带后缀，如000300.SH)
    | trade_dates: 历史交易（开市）日期数据或存档路径
    '''
    
    def _get_save_path(save_path):
        '''获取指数日线基本数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('index/tushare/{}/'.format(code),
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}_daily_basic.csv'.format(code)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)
    data_all = update_index_daily_basic(code,
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
    def try_update_index_daily_basic_check(*args, **kwargs):
        return update_index_daily_basic_check(*args, **kwargs)
    
    
    # tushare代码
    codes = {
        '000001.SH': '2004-01-01', # '上证指数'
        '399006.SZ': '2010-05-31', # '创业板指'
        '399005.SZ': '2006-12-26', # '中小板指'
        '000016.SH': '2004-01-01', # '上证50'
        '000300.SH': '2005-04-07', # '沪深300'
        '399300.SZ': '2005-04-07', # '沪深300'
        '000905.SH': '2007-01-14', # '中证500'
        '399001.SZ': '2004-01-01', # '深证成指'
    }
    keys = list(codes.keys())
    for k in range(len(keys)):
        code = keys[k]
        start_date = codes[code]
        if code.endswith('.SH'):
            trade_dates = trade_dates_sh
        elif code.endswith('.SZ'):
            trade_dates = trade_dates_sz
        exec('''df_{}, loss_{} = try_update_index_daily_basic_check(
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









