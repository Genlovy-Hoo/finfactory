# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
from dramkit import isnull
from dramkit import load_csv
from dramkit import logger_show
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import get_tushare_api
from finfactory.load_his_data import find_target_dir
from finfactory.config import cfg


global TS_API_USED_TIMES
TS_API_USED_TIMES = 0


def get_cols_info(fpath=None, root_dir=None):
    '''读取字段信息'''
    if isnull(fpath):
        fdir = find_target_dir('stocks/tushare/',
                               root_dir=root_dir)
        fpath = '{}cols_top10holders.xlsx'.format(fdir)
    if not os.path.exists(fpath):
        return None, None, None, None
    df_cols = pd.read_excel(fpath)
    cols = list(df_cols['存档名称'])
    cols_chn = list(df_cols['描述'])
    fields = ','.join(list(df_cols['名称']))
    cols_map = df_cols.set_index('名称')['存档名称'].to_dict()
    return cols, cols_chn, fields, cols_map

COLS, COLS_CHN, FIELDS, COLS_MAP = get_cols_info()


def get_fpath(code, fpath, save_csv, root_dir):
    if not isnull(fpath):
        return fpath
    if not save_csv:
        return None
    fdir = find_target_dir('stocks/tushare/{}/'.format(code),
                           root_dir=root_dir, make=True)
    return '{}{}_top10holders.csv'.format(fdir, code)


def _write_to_csv(df, code, fpath, save_csv, root_dir):
    fpath = get_fpath(code, fpath, save_csv, root_dir)
    if not isnull(fpath):
        df.to_csv(fpath, index=None, encoding='gbk')


def get_stock_top10holders(code,
                           start_date=None, end_date=None,
                           fpath=None, save_csv=False,
                           root_dir=None, ts_api=None,
                           logger=None):
    '''
    tushare获取股票前十大股东数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    if isnull(start_date):
        start_date = '19901218'
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    data = []
    for dt1, dt2 in dttools.cut_date(start_date, end_date, 1000):
        global TS_API_USED_TIMES
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_top10holders == 0:
            logger_show('{}, {}, pausing...'.format(code, dt1),
                        logger)
            time.sleep(61)
        df = ts_api.top10_holders(ts_code=code,
                                  start_date=dt1,
                                  end_date=dt2,
                                  fields=FIELDS)
        data.append(df)
    df = pd.concat(data, axis=0)
    for col in ['ann_date', 'end_date']:
        df[col] = df[col].apply(lambda x: dttools.date_reformat(x) if not isnull(x) else np.nan)
    df.rename(columns={'ts_code': 'code', 'ann_date': '公告日期',
                       'end_date': '报告期'},
              inplace=True)
    if not isnull(COLS):
        df.rename(columns=COLS_MAP, inplace=True)
        df = df[COLS]
    df.sort_values(['code', '报告期', 'holder_name'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    _write_to_csv(df, code, fpath, save_csv, root_dir)
    return df


def update_stock_top10holders(code, df_exist=None,
                              start_date=None, end_date=None,
                              fpath=None, root_dir=None,
                              ts_api=None, logger=None):
    '''增量更新股票前十大股东数据'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
        
    if isnull(start_date):
        start_date = '19901218'
    if isnull(end_date):
        end_date = dttools.today_date('')
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath, encoding='gbk')
    if not isnull(df_exist) and df_exist.shape[0] > 0:
        last_date = df_exist['报告期'].max()
        start_date = dttools.date_add_nday(last_date, 1)
    else:
        last_date = dttools.date_add_nday(start_date, -1)
            
    if not isnull(df_exist):
        df_exist = df_exist[COLS].copy()
    
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    last_date = dttools.date_reformat(last_date, '')
    
    if last_date >= end_date:
        logger_show('{}前十大股东最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        return df_exist
    
    logger_show('更新{}前十大股东数据, {}->{} ...'.format(code, start_date, end_date),
                logger, 'info')
    data = get_stock_top10holders(code,
                                  start_date=start_date,
                                  end_date=end_date,
                                  fpath=None,
                                  save_csv=False,
                                  root_dir=root_dir,
                                  ts_api=ts_api,
                                  logger=logger)
    if data.shape[0] == 0:
        logger_show('{}新获取0条记录，返回已存在数据。'.format(code),
                    logger, 'warn')
        return df_exist
    
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols=['code', '报告期',
                                       'holder_name'],
                            del_dup_cols=['code', '报告期',
                                          'holder_name'],
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None,
                            csv_encoding='gbk')
    data_all.reset_index(drop=True, inplace=True)
    
    return data_all


def get_save_path(code, save_path, root_dir, logger=None):
    '''获取股票前十大股东历史数据存放路径'''
    if isnull(save_path):
        save_dir = find_target_dir('stocks/tushare/{}/'.format(code),
                   root_dir=root_dir, make=True, logger=logger)
        save_path = '{}{}_top10holders.csv'.format(save_dir, code)
    return save_path


def update_stock_top10holders_check(code,
                                    save_path=None,
                                    start_date=None,
                                    root_dir=None,
                                    ts_api=None,
                                    logger=None):
    '''
    更新股票前十大股东数据
    '''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = get_save_path(code, save_path, root_dir, logger)    
    data_all = update_stock_top10holders(code,
                                         df_exist=None,
                                         start_date=start_date,
                                         end_date=None,
                                         fpath=save_path,
                                         root_dir=root_dir,
                                         ts_api=ts_api,
                                         logger=logger)
    
    return data_all


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from dramkit.iotools import get_last_change_time
    from finfactory.load_his_data import load_astocks_list_tushare
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api(cfg.tushare_token2)
    cfg.set_key_value('no_py_log', False)
    logger = gen_py_logger(sys.argv[0], config=cfg)
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_stock_top10holders_check(*args, **kwargs):
        return update_stock_top10holders_check(*args, **kwargs)
    
    
    # tushare代码
    # codes = {
    #     '000001.SZ': '1990-12-18'
    # }
    codes = load_astocks_list_tushare()
    codes = codes.set_index('code')['list_date'].to_dict()
    fpaths = {x: get_save_path(x, None, None, logger) for x in codes}
    chg_dates = {x: get_last_change_time(fpaths[x], '%Y-%m-%d') \
                    if os.path.exists(fpaths[x]) else None for x in fpaths}
    today = dttools.today_date('-')
    codes = {x: codes[x] for x in codes if isnull(chg_dates[x]) \
                or chg_dates[x] < today}

    dfs = {}
    keys = list(codes.keys())
    for k in range(len(keys)):
        code = keys[k]
        start_date = codes[code]
        logger_show('{}/{}, {}...'.format(k+1, len(keys), code),
                    logger)
        exec('''dfs['{}'] = try_update_stock_top10holders_check(
                                    code,
                                    save_path=None,
                                    start_date=start_date,
                                    root_dir=None,
                                    ts_api=ts_api,
                                    logger=logger)
              '''.format(code[:6])
            )
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









