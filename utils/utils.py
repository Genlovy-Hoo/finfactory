# -*- coding: utf-8 -*-

import os
import pandas as pd
import tushare as ts
import dramkit.datetimetools as dttools
from dramkit import load_csv, isnull, simple_logger
from dramkit.fintools.utils_chn import get_trade_dates
from dramkit.fintools.utils_chn import get_recent_trade_date
from dramkit.other.othertools import get_csv_df_colmaxmin
from finfactory.config import cfg


def gen_py_logger(pypath, logdir=None):
    '''根据Python文件路径生成对应的日志文件路径'''
    if cfg.no_py_log:
        # return None
        return simple_logger()
    if isnull(logdir):
        for fdir in cfg.log_dirs:
            if os.path.exists(fdir):
                logdir = fdir
                break
    if isnull(logdir):
        raise ValueError('未识别的`logdir`！')
    logpath = os.path.basename(pypath).replace('.py', '.log')
    logpath = os.path.join(logdir, logpath)
    logger = simple_logger(logpath, 'a')
    return logger


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
                            trade_dates=None,
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
        last_date = get_recent_trade_date(last_date, 'pre', trade_dates)
    else:
        if only_workday:
            last_date = dttools.get_recent_workday_chncal(last_date, 'pre')
        if only_inweek:
            last_date = dttools.get_recent_inweekday(last_date, 'pre')

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


def check_month_loss(df, month_col='month',
                     return_loss_data=False):
    if month_col is None:
        for col in ['month', 'time']:
            if col in df.columns:
                month_col = col
                break            
    month_min = str(df[month_col].min())
    month_max = str(df[month_col].max())
    assert len(month_min) == 6 and len(month_max) == 6
    date_min, date_max = month_min+'01', month_max+'01'
    date_all = pd.Series(pd.date_range(date_min, date_max))
    month_all = date_all.apply(lambda x: x.strftime('%Y%m'))
    month_all = list(set(month_all))
    month_all = pd.DataFrame(month_all,
                             columns=[month_col+'_all'])
    df_all = pd.merge(month_all, df, how='left',
                      left_on=month_col+'_all',
                      right_on=month_col)
    df_loss = df_all[df_all[month_col].isna()]
    if return_loss_data:
        return df_loss
    else:
        return df_loss[month_col+'_all'].unique().tolist()


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
                         default_last_date='19891231',
                         start_lag=1, **kwargs):
    '''
    tushare更新日线数据之前，根据存档路径save_path获取起止日期等变量
    '''
    if default_last_date is None:
        default_last_date = '19891231'
    # 获取存档数据和最后日期
    if (isinstance(save_path_df, pd.core.frame.DataFrame)) or \
       (isinstance(save_path_df, str) and os.path.exists(save_path_df)):
        last_date, _, data = get_csv_df_colmaxmin(save_path_df,
                                                  col=time_col,
                                                  **kwargs)
    else:
        last_date = default_last_date
        data = None
    # 日期格式转化为tushare接受的8位格式
    last_date = dttools.date_reformat(last_date, '')
    start_date = dttools.date_add_nday(last_date, start_lag)
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





