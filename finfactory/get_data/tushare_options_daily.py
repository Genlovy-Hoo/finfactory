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


COLS_FINAL = ['code', 'date', 'exchange', 'pre_settle',
              'pre_close', 'open', 'high', 'low', 'close',
              'settle', 'volume', 'amount', 'oi']

global TS_API_USED_TIMES
TS_API_USED_TIMES = 0


def check_loss(data, exchange, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('{}期权日线数据有缺失日期：'.format(exchange)+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def get_options_daily(exchange, date=None, ts_api=None):
    '''
    | tushare获取给定交易所exchange的期权日线数据
    | exchange表示交易所（与tushare官方文档一致）:
    |     CFFEX-中金所 DCE-大商所 CZCE-郑商所 SHFE-上期所
    |     SSE-上交所 SZSE-深交所
    | 注意：上交所深交所中金所合约比较少，一次能取当日所有日线，
    |      别的交易所有可能会出现一次取不全的情况（可能需要根据合约代码来取）！
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(date):
        date = get_recent_workday_chncal(dirt='pre')
    date = date_reformat(date, '')
    df = ts_api.opt_daily(exchange=exchange, trade_date=date)
    cols = {'ts_code': 'code', 'trade_date': 'date',
            'vol': 'volume'}
    df.rename(columns=cols, inplace=True)
    df['date'] = df['date'].apply(lambda x: date_reformat(x, '-'))
    df['amount'] = df['amount'] * 10000 # tushare单位为万元，转化为元
    return df[COLS_FINAL]


def get_options_daily_by_dates(exchange, dates,
                               ts_api=None, logger=None):
    if isnull(ts_api):
        ts_api = get_tushare_api()
    data = []
    for k in range(len(dates)):
        date = dates[k]
        logger_show('{}/{}, {} ...'.format(k, len(dates), date),
                    logger, 'info')
        data.append(get_options_daily(exchange, date, ts_api))
        if k % cfg.ts_1min_opt_daily == 0 and k > 0 and k != len(dates)-1:
            logger_show('pausing...', logger)
            time.sleep(61)
    data = pd.concat(data, axis=0)
    data.sort_values(['date', 'code'], inplace=True)
    return data


def update_options_daily(exchange, df_exist=None, fpath=None,
                         start_date='20150209', end_date=None,
                         trade_dates=None, ts_api=None,
                         logger=None):
    '''增量更新期权日线数据'''
    
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
        logger_show('{}期权日线最新数据已存在，不更新。'.format(exchange),
                    logger)
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)
    dates.sort()
    if len(dates) == 0:
        logger_show('{}期权日线最新数据已存在，不更新。'.format(exchange),
                    logger)
        return df_exist
    
    logger_show('更新{}期权日线数据, {}->{} ...'.format(exchange, dates[0], dates[-1]),
                logger, 'info')
    data = []
    for date in dates:
        global TS_API_USED_TIMES
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_opt_daily == 0:
            # 防止报错丢失，在迭代过程中保存数据
            if len(data) > 1:
                data_ = pd.concat(data, axis=0)[COLS_FINAL]
                if data_.shape[0] > 0:
                    _ = archive_data(data_, df_exist,
                                     sort_cols=['date', 'code'],
                                     del_dup_cols=['date', 'code'],
                                     sort_first=False,
                                     csv_path=fpath,
                                     csv_index=None)
            logger_show('pausing...', logger)
            time.sleep(61)
        logger_show('{}...'.format(date), logger)
        df = get_options_daily(exchange, date, ts_api)
        logger_show('{}, {}, {}'.format(exchange, date, df.shape),
                    logger)
        data.append(df)
            
    data = pd.concat(data, axis=0)
    if data.shape[0] == 0:
        logger_show('{}期权日线数据获取0条记录，返回已存在数据。'.format(exchange),
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
                            csv_index=None)
    data_all.reset_index(drop=True, inplace=True)
    
    return data_all


def update_options_daily_check(exchange,
                               save_path=None,
                               root_dir=None,
                               start_date='20150209',
                               trade_dates=None,
                               ts_api=None,
                               logger=None):
    '''
    更新交易所期权日线数据
    '''
    
    def _get_save_path(save_path):
        '''获取交易所期权日线数据存放路径'''
        if isnull(save_path):
            save_dir = find_target_dir('options/tushare/options_daily/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}.csv'.format(exchange)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)
    data_all = update_options_daily(exchange,
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
    
    ts_api = get_tushare_api(cfg.tushare_token2)
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_options_daily_check(*args, **kwargs):
        return update_options_daily_check(*args, **kwargs)
    
    
    exs = {
            'SSE': '20150209', # 上交所(上海证券交易所), .SH
            'SZSE': '20191223', # 深交所(深圳证券交易所), .SZ
            'CFFEX': '20191223', # 中金所(中国金融期权交易所), .CFX
            'CZCE': '20170419', # 郑商所(郑州商品交易所), .ZCE
            'SHFE': '20180921', # 上期所(上海期权交易所), .SHF
            'DCE': '20170331', # 大商所(大连商品交易所), .DCE
        }
    		
    dfs, losses = {}, {}
    exs_ = list(exs.keys())
    for k in range(len(exs_)):
        ex = exs_[k]
        start_date = exs[ex]
        trade_dates = load_trade_dates_tushare(ex)
        exec('''dfs['{}'], losses['{}'] = try_update_options_daily_check(
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
    
    
    
    
  
    
    
    
    