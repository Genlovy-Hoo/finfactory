# -*- coding: utf-8 -*-

import os
import time
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
from finfactory.config import cfg


global TS_API_USED_TIMES
TS_API_USED_TIMES = 0


def get_cols_info(fpath=None, root_dir=None):
    '''读取字段信息'''
    if isnull(fpath):
        fdir = find_target_dir('stocks/tushare/',
                               root_dir=root_dir)
        fpath = '{}cols_daily.xlsx'.format(fdir)
    if not os.path.exists(fpath):
        return None, None, None, None
    df_cols = pd.read_excel(fpath)
    cols = list(df_cols['存档名称'])
    cols_chn = list(df_cols['描述'])
    fields = ','.join(list(df_cols['名称']))
    cols_map = df_cols.set_index('名称')['存档名称'].to_dict()
    return cols, cols_chn, fields, cols_map

COLS, COLS_CHN, FIELDS, COLS_MAP = get_cols_info()


def check_loss(data, code, trade_dates, logger=None):
    '''检查缺失'''
    if isnull(data) or data.shape[0] == 0:
        logger_show('{}日线数据为空！'.format(code), logger, 'warn')
        return None
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        # logger_show('{}日线数据有缺失日期：'.format(code)+','.join(loss_dates),
        #             logger, 'warn')
        logger_show('{}日线数据缺失日期数：'.format(code)+str(len(loss_dates)),
                    logger, 'warn')
    return loss_dates


def get_fpath(code, fpath, save_csv, root_dir):
    if not isnull(fpath):
        return fpath
    if not save_csv:
        return None
    fdir = find_target_dir('stocks/tushare/{}/'.format(code),
                           root_dir=root_dir, make=True)
    return '{}{}_daily.csv'.format(fdir, code)


def _write_to_csv(df, code, fpath, save_csv, root_dir):
    fpath = get_fpath(code, fpath, save_csv, root_dir)
    if not isnull(fpath):
        df.to_csv(fpath, index=None)


def get_stock_daily(code, start_date=None, end_date=None,
                    fpath=None, save_csv=False,
                    root_dir=None, ts_api=None, logger=None):
    '''
    tushare获取股票日线数据
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
    global TS_API_USED_TIMES
    for dt1, dt2 in dttools.cut_date(start_date, end_date, 4500):
        df = ts_api.daily(ts_code=code,
                          start_date=dt1,
                          end_date=dt2,
                          fields=FIELDS)
        data.append(df)
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_daily == 0:
            logger_show('{}, {}, pausing...'.format(code, dt1),
                        logger)
            time.sleep(61)
    df = pd.concat(data, axis=0)
    df['vol'] = df['vol'] * 100 # 转化为股
    df['amount'] = df['amount'] * 1000 # 转化为元
    df['trade_date'] = df['trade_date'].apply(dttools.date_reformat)
    df.rename(columns={'ts_code': 'code', 'trade_date': 'date'},
              inplace=True)
    if not isnull(COLS):
        df.rename(columns=COLS_MAP, inplace=True)
        df = df[COLS]
    df.sort_values(['code', 'date'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    _write_to_csv(df, code, fpath, save_csv, root_dir)
    return df


def update_stock_daily(code, df_exist=None,
                       start_date=None, end_date=None,
                       fpath=None, root_dir=None,
                       trade_dates=None, ts_api=None,
                       logger=None):
    '''增量更新股票日线数据'''
    
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
        if not isnull(COLS):
            df_exist = df_exist[COLS].copy()
    if isnull(end_date):
        end_date = end_date_
    
    if last_date >= end_date:
        logger_show('{}日线最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)    
    if len(dates) == 0:
        logger_show('{}日线最新数据已存在，不更新。'.format(code),
                    logger, 'info')
        return df_exist
    
    logger_show('更新{}日线数据, {}->{} ...'.format(code, start_date, end_date),
                logger, 'info')
    data = get_stock_daily(
                    code, start_date=start_date,
                    end_date=end_date, fpath=None,
                    save_csv=False, root_dir=root_dir,
                    ts_api=ts_api, logger=logger)
    if data.shape[0] == 0:
        logger_show('{}新获取0条记录，返回已存在数据。'.format(code),
                    logger, 'warn')
        return df_exist
    
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols='date',
                            del_dup_cols='date',
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None)
    data_all.reset_index(drop=True, inplace=True)
    
    return data_all


def update_stock_daily_check(code,
                             save_path=None,
                             start_date=None,
                             root_dir=None,
                             trade_dates=None,
                             ts_api=None,
                             logger=None):
    '''
    | 更新股票日线行情数据
    | code: tushare代码(带后缀，如600570.SH)
    | trade_dates: 历史交易（开市）日期数据或存档路径
    '''
    
    def _get_save_path(save_path):
        '''获取股票日线历史数据存放路径'''
        if isnull(save_path):
            save_dir = find_target_dir('stocks/tushare/{}/'.format(code),
                       root_dir=root_dir, make=True, logger=logger)
            save_path = '{}{}_daily.csv'.format(save_dir, code)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_stock_daily(code,
                                  df_exist=None,
                                  start_date=start_date,
                                  end_date=None,
                                  fpath=save_path,
                                  root_dir=root_dir,
                                  trade_dates=trade_dates,
                                  ts_api=ts_api,
                                  logger=logger)
    
    loss_dates = check_loss(data_all, code, trade_dates, logger=logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.load_his_data import load_astocks_list_tushare
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    cfg.set_key_value('no_py_log', False)
    logger = gen_py_logger(sys.argv[0], config=cfg)
    
    trade_dates_sh = load_trade_dates_tushare('SSE')
    trade_dates_sz = load_trade_dates_tushare('SZSE')
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_stock_daily_check(*args, **kwargs):
        return update_stock_daily_check(*args, **kwargs)
    
    
    # tushare代码
    # codes = {
    #     '000001.SZ': '1990-12-18'
    # }
    codes = load_astocks_list_tushare()
    codes = codes.set_index('code')['list_date'].to_dict()

    dfs, losses = {}, {}
    keys = list(codes.keys())
    for k in range(len(keys)):
        code = keys[k]
        start_date = codes[code]
        if code.endswith('.SH'):
            trade_dates = trade_dates_sh
        elif code.endswith('.SZ'):
            trade_dates = trade_dates_sz
        elif code.endswith('BJ'):
            trade_dates = trade_dates_sh
        logger_show('{}/{}, {}...'.format(k+1, len(keys), code),
                    logger)
        exec('''dfs['{}'], losses['{}'] = try_update_stock_daily_check(
                                    code,
                                    save_path=None,
                                    start_date=start_date,
                                    root_dir=None,
                                    trade_dates=trade_dates,
                                    ts_api=ts_api,
                                    logger=logger)
              '''.format(code[:6], code[:6])
            )
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









