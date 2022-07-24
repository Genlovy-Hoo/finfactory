# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from dramkit import logger_show, isnull, load_csv
from dramkit.datetimetools import date_reformat
from dramkit.datetimetools import get_recent_workday_chncal
from dramkit.other.othertools import archive_data
from finfactory.fintools.utils_chn import get_trade_dates
from finfactory.load_his_data import find_target_dir
from finfactory.utils.utils import get_tushare_api
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import parms_check_ts_daily
from finfactory.config import cfg


COLS_FINAL = ['code', 'date', 'open', 'high', 'low', 'close',
              'settle', 'volume', 'amount', 'pre_close',
              'pre_settle', '收盘价_昨结算价', '结算价_昨结算价',
              '持仓量(手)', '持仓量变化', '交割结算价']

global TS_API_USED_TIMES
TS_API_USED_TIMES = 0


def check_loss(data, exchange, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('{}期货日线数据有缺失日期：'.format(exchange)+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def get_futures_daily(exchange, date=None, ts_api=None):
    '''
    | tushare获取给定交易所exchange的期货日线数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(date):
        date = get_recent_workday_chncal(dirt='pre')
    date = date_reformat(date, '')
    cols = {'ts_code': 'code',
            'trade_date': 'date',
            'pre_close': 'pre_close',
            'pre_settle': 'pre_settle',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'settle': 'settle',
            'change1': '收盘价_昨结算价',
            'change2': '结算价_昨结算价',
            'vol': 'volume',
            'amount': 'amount',
            'oi': '持仓量(手)',
            'oi_chg': '持仓量变化',
            'delv_settle': '交割结算价'
        }
    df = ts_api.fut_daily(exchange=exchange, trade_date=date,
                          fields=','.join(list(cols.keys())))
    df.rename(columns=cols, inplace=True)
    df['amount'] = df['amount'] * 10000 # tushare期货行情成交额单位为万元
    df['date'] = df['date'].apply(lambda x: date_reformat(x, '-'))
    df = df[COLS_FINAL]
    return df


def get_futures_daily_by_dates(exchange, dates,
                               ts_api=None, logger=None):
    if isnull(ts_api):
        ts_api = get_tushare_api()
    data = []
    for k in range(len(dates)):
        date = dates[k]
        logger_show('{}/{}, {} ...'.format(k, len(dates), date),
                    logger, 'info')
        data.append(get_futures_daily(exchange, date, ts_api))
        if k % cfg.ts_1min_fut_daily == 0 and k > 0 and k != len(dates)-1:
            logger_show('pausing...', logger)
            time.sleep(61)
    data = pd.concat(data, axis=0)
    data.sort_values(['date', 'code'], inplace=True)
    return data


def update_futures_daily(exchange, df_exist=None, fpath=None,
                         start_date='19950417', end_date=None,
                         trade_dates=None, ts_api=None,
                         logger=None):
    '''增量更新期货日线数据'''
    
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
                                 start_lag=1)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
    if isnull(end_date):
        end_date = end_date_
        
    if last_date >= end_date:
        logger_show('{}期货日线最新数据已存在，不更新。'.format(exchange),
                    logger)
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)
    dates.sort()
    if len(dates) == 0:
        logger_show('{}期货日线最新数据已存在，不更新。'.format(exchange),
                    logger)
        return df_exist
    
    logger_show('更新{}期货日线数据, {}->{} ...'.format(exchange, dates[0], dates[-1]),
                logger, 'info')
    data = []
    global TS_API_USED_TIMES
    for date in dates:
        logger_show('{}...'.format(date), logger)
        df = get_futures_daily(exchange, date, ts_api)
        logger_show('{}, {}, {}'.format(exchange, date, df.shape),
                    logger)
        data.append(df)
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_fut_daily == 0:
            # 防止报错丢失，在迭代过程中保存数据
            if len(data) > 1:
                data_ = pd.concat(data, axis=0)[COLS_FINAL]
                if data_.shape[0] > 0:
                    _ = archive_data(data_, df_exist,
                                     sort_cols=['date', 'code'],
                                     del_dup_cols=['date', 'code'],
                                     sort_first=False,
                                     csv_path=fpath,
                                     csv_index=None,
                                     csv_encoding='gbk')
            logger_show('pausing...', logger)
            time.sleep(61)
            
    data = pd.concat(data, axis=0)
    if data.shape[0] == 0:
        logger_show('{}期货日线数据获取0条记录，返回已存在数据。'.format(exchange),
                    logger, 'warn')
        if os.path.exists(fpath):
            return load_csv(fpath)
        else:
            return None
    
    # 统一字段名    
    data = data[COLS_FINAL].copy()
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols=['date', 'code'],
                            del_dup_cols=['date', 'code'],
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None,
                            csv_encoding='gbk')
    data_all.reset_index(drop=True, inplace=True)
    
    return data_all


def update_futures_daily_check(exchange,
                               save_path=None,
                               root_dir=None,
                               start_date='19950417',
                               trade_dates=None,
                               ts_api=None,
                               logger=None):
    '''
    更新交易所期货日线数据
    '''
    
    def _get_save_path(save_path):
        '''获取交易所期货日线数据存放路径'''
        if isnull(save_path):
            save_dir = find_target_dir('futures/tushare/futures_daily/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}.csv'.format(exchange)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)
    data_all = update_futures_daily(exchange,
                                    df_exist=None,
                                    fpath=save_path,
                                    start_date=start_date,
                                    end_date=None,
                                    trade_dates=trade_dates,
                                    ts_api=ts_api,
                                    logger=logger)
    
    loss_dates = check_loss(data_all, exchange, trade_dates, logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_futures_daily_check(*args, **kwargs):
        return update_futures_daily_check(*args, **kwargs)
    
    
    exs = {
            'CFFEX': '20100416', # '中金所',
            'DCE': '19990104', # '大商所',
            'CZCE': '19990104', # '郑商所',
            'SHFE': '19950417', # '上期所',
            'INE': '20180326', # '上海国际能源交易中心',
        }
    		
    dfs, losses = {}, {}
    exs_ = list(exs.keys())
    for k in range(len(exs_)):
        ex = exs_[k]
        start_date = exs[ex]
        trade_dates = load_trade_dates_tushare(ex)
        exec('''dfs['{}'], losses['{}'] = try_update_futures_daily_check(
                                        ex,
                                        save_path=None,
                                        root_dir=None,
                                        start_date=start_date,
                                        trade_dates=trade_dates,
                                        ts_api=ts_api,
                                        logger=logger)
              '''.format(ex, ex)
              )
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    

    
    
    
    