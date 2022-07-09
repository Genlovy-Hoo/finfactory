# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
from dramkit import isnull, logger_show, load_csv
from dramkit.other.othertools import archive_data
from dramkit.datetimetools import date_reformat
from dramkit.datetimetools import get_recent_workday_chncal
from dramkit.iotools import get_last_change_time
from finfactory.utils.utils import get_tushare_api
from finfactory.load_his_data import find_target_dir


def get_cols_info(fpath=None, root_dir=None):
    '''读取字段信息'''
    if isnull(fpath):
        fdir = find_target_dir('stocks/tushare/',
                               root_dir=root_dir)
        fpath = '{}cols_astocks_list.xlsx'.format(fdir)
    if not os.path.exists(fpath):
        return None, None, None, None
    df_cols = pd.read_excel(fpath)
    cols = list(df_cols['存档名称'])
    cols_chn = list(df_cols['描述'])
    fields = ','.join(list(df_cols['名称']))
    cols_map = df_cols.set_index('名称')['存档名称'].to_dict()
    return cols, cols_chn, fields, cols_map


def get_astocks_list(cols_path=None, root_dir=None,
                     ts_api=None):
    '''
    tushare获取A股基本信息
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    cols, _, fields, cols_map = get_cols_info(cols_path, root_dir)
    part1 = ts_api.stock_basic(list_status='L', fields=fields)
    part2 = ts_api.stock_basic(list_status='D', fields=fields)
    part3 = ts_api.stock_basic(list_status='P', fields=fields)
    df = pd.concat((part1, part2, part3), axis=0)
    df['symbol'] = df['symbol'].apply(lambda x: str(x).zfill(6))
    if not isnull(cols):
        df.rename(columns=cols_map, inplace=True)
        df = df[cols]
    for col in ['list_date', 'delist_date']:
        df[col] = df[col].apply(lambda x:
                  date_reformat(str(int(x)), '-') if not isnull(x) else np.nan)
    return df


def update_astocks_list(save_path=None, root_dir=None,
                        cols_path=None, ts_api=None,
                        logger=None):
    '''更新A股列表基本信息'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if save_path is None:
        save_dir = find_target_dir('stocks/tushare/',
                   root_dir=root_dir, make=True, logger=logger)
        save_path = save_dir + 'astocks_list.csv'
    assert not isnull(save_path), '`save_path`不能为无效值！'
    
    end_date = get_recent_workday_chncal(dirt='pre')    
    if os.path.exists(save_path) and \
       (get_last_change_time(save_path, '%Y-%m-%d') >= end_date):
        logger_show('A股列表数据已是最新，不更新！', logger)
        return load_csv(save_path, encoding='gbk')
    
    logger_show('更新A股列表数据...', logger)
    df = get_astocks_list(cols_path, root_dir, ts_api)
    if df.shape[0] == 0 or isnull(df):
        logger_show('获取0条记录！', logger, 'warn')
        if os.path.exists(save_path):
            return load_csv(save_path, encoding='gbk')
        else:
            return None
    
    if os.path.exists(save_path):
        df_exist = load_csv(save_path, encoding='gbk')
    else:
        df_exist = None
    # 数据合并
    df = archive_data(df, df_exist,
                      sort_cols=['code', 'list_date'],
                      del_dup_cols=['code', 'list_date'],
                      sort_first=True,
                      csv_path=save_path,
                      csv_index=None,
                      csv_encoding='gbk')
    df.reset_index(drop=True, inplace=True)
    
    return df


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_astocks_list(*args, **kwargs):
        return update_astocks_list(*args, **kwargs)
    
    
    df = try_update_astocks_list(
                            save_path=None,
                            root_dir=None,
                            cols_path=None,
                            ts_api=ts_api,
                            logger=logger)
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
