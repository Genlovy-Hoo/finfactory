# -*- coding: utf-8 -*-

import os
import pandas as pd
import tushare as ts
from dramkit import load_csv
import dramkit.datetimetools as dttools
from dramkit.fintools.utils_chn import get_trade_dates
from dramkit.other.othertools import get_csv_df_colmaxmin


def check_date_loss(df, date_col=None,
                    only_workday=True, del_weekend=True,
                    return_loss_data=False,
                    trade_dates_df_path=None):
    '''检查df中日期列缺失情况'''
    if date_col is None:
        for col in ['date', 'time']:
            if col in df.columns:
                date_col = col
                break
    date_min = df[date_col].min()
    date_max = df[date_col].max()
    if not trade_dates_df_path is None:
        date_all = get_trade_dates(date_min, date_max, trade_dates_df_path)
    else:
        date_all = dttools.get_dates_between(date_min, date_max,
                                             keep1=True, keep2=True,
                                             only_workday=only_workday,
                                             del_weekend=del_weekend)
    date_all = pd.DataFrame(date_all, columns=['date_all'])
    df_all = pd.merge(date_all, df, how='left',
                      left_on='date_all', right_on=date_col)
    df_loss = df_all[df_all[date_col].isna()]
    if return_loss_data:
        return df_loss
    else:
        return df_loss['date_all'].unique().tolist()
    
    
def check_daily_data_is_new(df_path,
                            date_col='date',
                            only_trade_day=True,
                            only_workday=True,
                            only_inweek=True,
                            return_data=False):
    '''
    | 检查日频数据是否为最新值
    | 注：若date_col列不存在，则默认将索引作为日期
    '''

    if isinstance(df_path, str) and os.path.isfile(df_path):
        data = load_csv(df_path)
    elif isinstance(df_path, pd.core.frame.DataFrame):
        data = df_path.copy()
    else:
        raise ValueError('df_path不是pd.DataFrame或路径不存在！')

    try:
        exist_last_date = data[date_col].max()
    except:
        exist_last_date = data.index.max()
    _, joiner = dttools.get_date_format(exist_last_date)

    last_date = dttools.today_date(joiner)
    if only_trade_day:
        last_date = dttools.get_recent_trade_date_chncal(last_date, 'pre')
    else:
        if only_workday:
            last_date = dttools.get_recent_workday_chncal(last_date, 'pre')
        if only_inweek:
            last_date = dttools.get_recent_inweekday_chncal(last_date, 'pre')

    if exist_last_date == last_date:
        if return_data:
            return True, (None, exist_last_date, last_date), data
        else:
            return True, (None, exist_last_date, last_date)
    elif exist_last_date < last_date:
        if return_data:
            return False, ('未到最新日期', exist_last_date, last_date), data
        else:
            return False, ('未到最新日期', exist_last_date, last_date)
    elif exist_last_date > last_date:
        if return_data:
            return False, ('超过最新日期', exist_last_date, last_date), data
        else:
            return False, ('超过最新日期', exist_last_date, last_date)


def check_minute_loss(df, freq='1min', time_col=None,
                      only_workday=True, del_weekend=True):
    '''检查df中的时间列缺失情况'''
    raise NotImplementedError
    
    
def get_tushare_api(token=None):
    '''根据token获取tushare API接口'''
    if token is None:
        from finfactory.config import cfg
        token = cfg.tushare_token1
    ts.set_token(token)
    ts_api = ts.pro_api()
    return ts_api


def parms_check_ts_daily(save_path_df, time_col='date',
                         default_last_date='19891231'):
    '''
    tushare更新日线数据之前，根据存档路径save_path获取起止日期等变量
    '''
    if default_last_date is None:
        default_last_date = '19891231'
    # 获取存档数据和最后日期
    if isinstance(save_path_df, pd.core.frame.DataFrame) or \
       os.path.exists(save_path_df):
        last_date, _, data = get_csv_df_colmaxmin(save_path_df,
                                                  col=time_col)
    else:
        last_date = default_last_date
        data = None
    # 日期格式转化为tushare接受的8位格式
    last_date = dttools.date_reformat(last_date, '')
    start_date = dttools.date_add_nday(last_date, 1)
    end_date = dttools.today_date('') # 默认更新到当日数据
    return last_date, start_date, end_date, data


def get_gm_api(token=None):
    '''根据token获取掘金API接口'''
    import gm.api as gm_api
    if token is None:
        from finfactory.config import cfg
        token = cfg.gm_token
    gm_api.set_token(token)
    return gm_api





