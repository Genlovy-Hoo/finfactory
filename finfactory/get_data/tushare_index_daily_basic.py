# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from dramkit import isnull
from dramkit import load_csv
from dramkit import logger_show
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from dramkit.fintools.utils_chn import get_trade_dates
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import parms_check_ts_daily


COLS_FINAL = ['code', 'date', '总市值', '流通市值', '总股本', '流通股本',
              '自由流通股本', '换手率', '换收益(自由流通)', 'pe', 'pe_ttm', 'pb']


def _check_loss(data, code, trade_dates_df_path, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates_df_path)
    if len(loss_dates) > 0:
        logger_show('{}日基本数据有缺失日期：'.format(code)+','.join(loss_dates),
                    logger, 'warn')
        
        
def get_index_daily_basic(ts_api, code, start_date, end_date=None):
    '''
    | tushare获取指数日基本数据
    | start_date和end_date格式：'20220610'
    '''
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
    df = df.reindex(columns=COLS_FINAL)
    return df


def update_index_daily_basic_by_exist(ts_api, df_exist,
                                      default_last_date=None,
                                      trade_dates=None,
                                      logger=None):
    '''增量更新指数日基本数据'''
    
    assert not isnull(df_exist), '`df_exist`必须为df！'
    code = df_exist['code'].unique().tolist()[0]
    
    df_exist = df_exist.reindex(columns=COLS_FINAL)
    
    last_date, start_date, end_date, df_exist = parms_check_ts_daily(
                           df_exist, default_last_date=default_last_date)
    
    if last_date >= end_date:
        logger_show('{}日基本数据最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        _check_loss(df_exist, code, trade_dates, logger=logger)
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)    
    if len(dates) == 0:
        logger_show('{}日基本数据最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        _check_loss(df_exist, code, trade_dates, logger=logger)
        return df_exist
    
    logger_show('更新{}日基本数据, {}->{} ...'.format(code, dates[0], dates[-1]),
                logger, 'info')
    data = []
    for date1, date2 in dttools.cut_date(start_date, end_date, 2000):
        df = get_index_daily_basic(ts_api, code, date1, date2)
        data.append(df)
    data = pd.concat(data, axis=0)
    if data.shape[0] == 0:
        logger_show('{}新获取0条记录，返回已存在数据。'.format(code),
                    logger, 'warn')
        _check_loss(df_exist, code, trade_dates, logger=logger)
        return df_exist
    
    # 统一字段名    
    data = data.reindex(columns=COLS_FINAL)
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols='date',
                            del_dup_cols='date',
                            sort_first=False)
    _check_loss(data_all, code, trade_dates, logger=logger)
    
    return data_all


def update_index_daily_basic(ts_api, code,
                             save_path=None,
                             root_dir=None,
                             trade_dates=None,
                             default_last_date='20040101',
                             logger=None):
    '''
    | 更新指数日基本数据
    | code: tushare代码(带后缀，如000300.SH)
    | trade_dates_path: 历史交易（开市）日期数据存档路径
    | default_last_date: code对应标的第一个交易日的前一天
    '''
    
    def _load_exist_index_daily_basic(save_path=None, root_dir=None):
        '''导入已存在的指数日线历史数据'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('index/tushare/{}/'.format(code),
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}_daily_basic.csv'.format(code)
        data = load_csv(save_path, encoding='gbk', logger=logger)
        if not data is None:
            data.sort_values('date', ascending=True, inplace=True)
            data.drop_duplicates(subset=['date'], keep='last', inplace=True)
        return data, save_path
    
    data_exist, save_path = _load_exist_index_daily_basic(save_path, root_dir)
    no_exist = False
    if data_exist is None:
        no_exist = True
        if default_last_date is None:
            default_last_date = '19891231'
        _tmp = dttools.date_reformat(default_last_date, '-')
        data_exist = pd.DataFrame([[code, _tmp]+[np.nan]*(len(COLS_FINAL)-2)],
                                  columns=COLS_FINAL)
    
    data_all =  update_index_daily_basic_by_exist(ts_api, data_exist,
                                                  default_last_date=default_last_date,
                                                  trade_dates=trade_dates,
                                                  logger=logger) 
    if no_exist:
        data_all = data_all.iloc[1:, :]
    data_all.to_csv(save_path, index=None, encoding='gbk')
    
    return data_all


if __name__ == '__main__':
    import time
    from dramkit import simple_logger, close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import find_target_dir
    from finfactory.utils.utils import get_tushare_api
    from finfactory.config import cfg  
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = simple_logger('../../log/get_data_ts_index_daily_basic.log', 'a')
    
    trade_dates_sh = find_target_dir('trade_dates/') + 'SSE.csv'
    trade_dates_sh = load_csv(trade_dates_sh)
    trade_dates_sz = find_target_dir('trade_dates/') + 'SZSE.csv'
    trade_dates_sz = load_csv(trade_dates_sz)
    
    
    @try_repeat_run(cfg.try_get_data_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_data_sleep_tushare)
    def try_update_index_daily_basic(*args, **kwargs):
        return update_index_daily_basic(*args, **kwargs)
    
    
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
        last_date = codes[code]
        if code.endswith('.SH'):
            trade_dates = trade_dates_sh
        elif code.endswith('.SZ'):
            trade_dates = trade_dates_sz
        df = try_update_index_daily_basic(ts_api, code,
                                    save_path=None,
                                    root_dir=None,
                                    trade_dates=trade_dates,
                                    default_last_date=last_date,
                                    logger=logger)
        # if k % 5 == 0:
        #     time.sleep(61)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
