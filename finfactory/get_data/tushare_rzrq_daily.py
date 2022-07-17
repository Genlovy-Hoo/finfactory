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
from finfactory.load_his_data import find_target_dir


COLS_FINAL = ['date', 'exchange', 'rzye', 'rzmre',
              'rzche', 'rqye', 'rqmcl', 'rzrqye', 'rqyl']


def check_loss(data, exchange, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('{}融资融券数据有缺失日期：'.format(exchange)+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def get_rzrq_daily(exchange, start_date,
                   end_date=None, ts_api=None):
    '''
    | tushare获取融资融券数据
    | start_date和end_date格式：'20220610'
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    cols = {'trade_date': 'date',
            'exchange_id': '交易所',
            'rzye': '融资余额(元)',
            'rzmre': '融资买入额(元)',
            'rzche': '融资偿还额(元)',
            'rqye': '融券余额(元)',
            'rqmcl': '融券卖出量(股,份,手)',
            'rzrqye': '融资融券余额(元)',
            'rqyl': '融券余量(股,份,手)'}
    cols_rename = {'trade_date': 'date',
                   'exchange_id': 'exchange'}
    data = []
    for date1, date2 in dttools.cut_date(start_date, end_date, 2000):
        df = ts_api.margin(exchange_id=exchange,
                           start_date=date1,
                           end_date=date2,
                           fields=','.join(list(cols.keys())))
        data.append(df)
    df = pd.concat(data, axis=0)    
    df.rename(columns=cols_rename, inplace=True)
    df['date'] = df['date'].apply(dttools.date_reformat)
    df = df[COLS_FINAL]
    df.sort_values('date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def update_rzrq_daily(exchange, df_exist=None, fpath=None,
                      start_date='20100330', end_date=None,
                      trade_dates=None, ts_api=None, 
                      logger=None):
    '''增量更新融资融券数据'''
    
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
        
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    last_date = dttools.date_reformat(last_date, '')
    
    if last_date >= end_date:
        logger_show('{}融资融券最新数据已存在，不更新。'.format(exchange),
                    logger, 'info')
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)    
    if len(dates) == 0:
        logger_show('{}融资融券最新数据已存在，不更新。'.format(exchange),
                    logger, 'info')
        return df_exist
    
    logger_show('更新{}融资融券数据, {}->{} ...'.format(exchange, dates[0], dates[-1]),
                logger, 'info')
    data = get_rzrq_daily(exchange, start_date, end_date, ts_api)
    if data.shape[0] == 0:
        logger_show('{}新获取0条记录，返回已存在数据。'.format(exchange),
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
    data_all.reset_index(drop=True, inplace=True)
    
    return data_all


def update_rzrq_daily_check(exchange,
                            save_path=None,
                            root_dir=None,
                            start_date='20100330',
                            trade_dates=None,
                            ts_api=None,
                            logger=None):
    '''
    更新融资融券行情数据
    '''
    
    def _get_save_path(save_path):
        '''获取融资融券历史数据存放路径'''
        if isnull(save_path):
            save_dir = find_target_dir('rzrq/tushare/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}.csv'.format(exchange)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_rzrq_daily(exchange,
                                 df_exist=None,
                                 fpath=save_path,
                                 start_date=start_date,
                                 end_date=None,
                                 trade_dates=trade_dates,
                                 ts_api=ts_api,
                                 logger=logger)
    
    logger_show('{}融资融券日数据最新日期：{}'.format(exchange, data_all["date"].max()),
                logger, 'info')
    loss_dates = check_loss(data_all, exchange, trade_dates, logger=logger)
    
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
    def try_update_rzrq_daily_check(*args, **kwargs):
        return update_rzrq_daily_check(*args, **kwargs)
    
    
    # tushare代码
    exs = {
        'SSE': '2010-03-31', # '上交所'
        'SZSE': '2010-03-31', # '深交所'
    }
    
    dfs, losses = {}, {}
    keys = list(exs.keys())
    for k in range(len(keys)):
        ex = keys[k]
        start_date = exs[ex]
        if ex.endswith('SSE'):
            trade_dates = trade_dates_sh
        elif ex.endswith('SZSE'):
            trade_dates = trade_dates_sz
        exec('''dfs['{}'], losses['{}'] = try_update_rzrq_daily_check(
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









