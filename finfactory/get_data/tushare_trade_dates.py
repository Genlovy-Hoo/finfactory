# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import dramkit.datetimetools as dttools
from dramkit import isnull, logger_show, load_csv
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import parms_check_ts_daily


COLS_FINAL = ['exchange', 'date', 'is_open', 'pre_trade_date']


def _check_loss(data, exchange, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 only_workday=False,
                                 del_weekend=False)
    if len(loss_dates) > 0:
        logger_show('{}交易日数据有缺失日期：'.format(exchange)+','.join(loss_dates),
                    logger, 'warn')
        
        
def get_his_trade_dates(ts_api, exchange, start_date, end_date=None):
    '''
    | tushare获取历史交易日数据
    | start_date和end_date格式：'20220611'
    | exchange表示交易所（与tushare官方文档一致，默认上交所）:
    |     SSE上交所, SZSE深交所, CFFEX中金所, SHFE上期所,
    |     CZCE郑商所, DCE大商所, INE上能源
    | exchange若为None，则默认上交所
    '''
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
    return df


def update_trade_dates_by_exist(ts_api, data_exist,
                                default_last_date=None,
                                logger=None):
    '''增量更新历史交易日数据'''
    exchange = data_exist['exchange'].unique().tolist()[0]
    last_date, start_date, end_date, df_exist = parms_check_ts_daily(
                           data_exist, default_last_date=default_last_date)
    
    if last_date >= end_date:
        logger_show('{}交易日历最新数据已存在，不更新。'.format(exchange),
                    logger, 'info')
        _check_loss(df_exist, exchange, logger=logger)
        return df_exist
    
    logger_show('更新{}历史交易日期数据, {}->{}...'.format(
                exchange, start_date, end_date),
                logger, 'info')
    df = get_his_trade_dates(ts_api, exchange, start_date, end_date)
    
    # 数据合并
    data_all = archive_data(df, df_exist,
                            sort_cols='date',
                            del_dup_cols='date',
                            sort_first=False)
    _check_loss(data_all, exchange, logger=logger)
    
    return data_all


def update_trade_dates(ts_api, exchange='SSE',
                       default_last_date='19891231',
                       save_path=None, root_dir=None,
                       logger=None):
    '''
    | 更新交易日期数据
    | exchange表示交易所（与tushare官方文档一致，默认上交所）:
    |     SSE上交所, SZSE深交所, CFFEX中金所, SHFE上期所,
    |     CZCE郑商所, DCE大商所, INE上能源
    '''
    
    def _load_exist_trade_dates(save_path=None, root_dir=None):
        '''导入已存在的交易日历史数据'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('trade_dates/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}.csv'.format(exchange)
        data = load_csv(save_path, logger=logger)
        if not data is None:
            data.sort_values('date', ascending=True, inplace=True)
            data.drop_duplicates(subset=['date'], keep='last', inplace=True)
        return data, save_path
    
    data_exist, save_path = _load_exist_trade_dates(save_path, root_dir)
    no_exist = False
    if data_exist is None:
        no_exist = True
        if default_last_date is None:
            default_last_date = '19891231'
        _tmp = dttools.date_reformat(default_last_date, '-')
        data_exist = pd.DataFrame([[exchange, _tmp, 0, np.nan]],
                                  columns=COLS_FINAL)
    
    data_all =  update_trade_dates_by_exist(ts_api, data_exist,
                default_last_date=default_last_date, logger=logger)   
    if no_exist:
        data_all = data_all.iloc[1:, :]
    
    data_all.to_csv(save_path, index=None)
    
    return data_all


if __name__ == '__main__':
    import time
    from dramkit import simple_logger, close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.utils.utils import get_tushare_api
    from finfactory.config import cfg  
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = simple_logger('../../log/get_data_ts_trade_dates.log', 'a')
    
    
    @try_repeat_run(cfg.try_get_data_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_data_sleep_tushare)
    def try_update_trade_dates(*args, **kwargs):
        return update_trade_dates(*args, **kwargs)
    
    
    exs = {
            'SSE': '1990-12-18', # 上交所
            'SZSE': '1991-07-02', # 深交所
            'CFFEX': '2006-09-07', # 中金所
            'SHFE': '1991-05-27', # 上期所
            'CZCE': '1990-10-11', # 郑商所
            'DCE': '1993-02-28', # 大商所
            'INE': '2017-05-22' # 上能源
        }
    for ex, last_date in exs.items():
        df = try_update_trade_dates(ts_api, ex,
                                    default_last_date=last_date,
                                    save_path=None,
                                    root_dir=None,
                                    logger=logger)
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
