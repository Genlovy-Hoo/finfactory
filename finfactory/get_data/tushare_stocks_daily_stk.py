# -*- coding: utf-8 -*-

import os
import time
import datetime
import pandas as pd
from dramkit import isnull, load_csv
from dramkit import logger_show
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.fintools.utils_chn import get_trade_dates
from finfactory.utils.utils import check_date_loss
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
        fpath = '{}cols_daily_stk_factor.xlsx'.format(fdir)
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


def get_fpath(code, fpath=None, root_dir=None, logger=None):
    if not isnull(fpath):
        return fpath
    fdir = find_target_dir('stocks/tushare/{}/'.format(code),
                           root_dir=root_dir, make=True,
                           logger=logger)
    return '{}{}_daily_stk.csv'.format(fdir, code)


def get_stocks_daily_stk(start_date=None, end_date=None,
                         fpath=None,  trade_dates=None,
                         ts_api=None, logger=None):
    '''
    tushare获取指定日期股票日线数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    if isnull(start_date):
        start_date = '19901218'
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    dates = get_trade_dates(start_date, end_date, trade_dates)
    data = []
    for date in dates:
        global TS_API_USED_TIMES
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_stk_factor == 0:
            logger_show('{}, pausing...'.format(date),
                        logger)
            time.sleep(61)
        df = ts_api.stk_factor(trade_date=date,
                               fields=FIELDS)
        data.append(df)
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
    if not isnull(fpath):
        df.to_csv(fpath, index=None)
    return df


def update_stocks_daily_stk(df_exist=None,
                            start_date=None,
                            end_date=None,
                            fpath=None,
                            trade_dates=None,
                            ts_api=None,
                            logger=None):
    '''增量更新股票日线数据'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if isnull(start_date):
        start_date = '19901218'
    if isnull(end_date):
        end_date = dttools.today_date('')
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath)

    if not isnull(df_exist):
        if not isnull(COLS):
            df_exist = df_exist[COLS].copy()
    if not isnull(df_exist) and df_exist.shape[0] > 0:
        # last_date = df_exist.groupby('code').max()['date'].min()
        last_date = df_exist.groupby('code').max()['date'].max()
        start_date = dttools.date_add_nday(last_date, 1)
    else:
        last_date = dttools.date_add_nday(start_date, -1)
    
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    last_date = dttools.date_reformat(last_date, '')
    
    if last_date >= end_date:
        logger_show('股票日线最新数据已存在，不更新。', logger)
        return df_exist
    
    dates = get_trade_dates(start_date, end_date, trade_dates)    
    if len(dates) == 0:
        logger_show('股票日线最新数据已存在，不更新。', logger)
        return df_exist
    
    logger_show('更新股票日线数据, {}->{} ...'.format(start_date, end_date),
                logger, 'info')
    data = get_stocks_daily_stk(start_date=start_date,
                                end_date=end_date,
                                fpath=None,
                                trade_dates=trade_dates,
                                ts_api=ts_api,
                                logger=logger)
    if data.shape[0] == 0:
        logger_show('新获取0条记录，返回已存在数据。',
                    logger, 'warn')
        return df_exist
    
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols=['date', 'code'],
                            del_dup_cols=['date', 'code'],
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None)
    data_all.reset_index(drop=True, inplace=True)
    
    return data_all


def update_stocks_daily_stk_check(save_path=None,
                                  start_date=None,
                                  root_dir=None,
                                  trade_dates=None,
                                  split=True,
                                  ts_api=None,
                                  logger=None):
    '''
    更新股票日线数据
    '''
    
    def _get_save_path(save_path):
        '''获取股票日线历史数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('stocks/tushare/stocks_daily/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = '{}astocks_daily_stk.csv'.format(save_dir)
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_stocks_daily_stk(df_exist=None,
                                       start_date=start_date,
                                       end_date=None,
                                       fpath=save_path,
                                       trade_dates=trade_dates,
                                       ts_api=ts_api,
                                       logger=logger)
    
    loss_dates = check_loss(data_all, '股票', trade_dates, logger=logger)
    
    # 数据拆分为为每只股票一个文件
    codes = data_all['code'].unique().tolist()
    df_stocks, loss_stocks = {}, {}
    if split:
        for k in range(len(codes)):
            if k % 100 == 0:
                logger_show('{}/{} data spliting...'.format(k+1, len(codes)),
                            logger, 'info')
            code = codes[k]
            df = data_all[data_all['code'] == code].copy()
            fpath = get_fpath(code, fpath=None,
                              root_dir=root_dir, logger=logger)
            if os.path.exists(fpath):
                df0 = load_csv(fpath)
            else:
                df0 = None
                logger_show('{}无历史已存在数据'.format(code),
                            logger)
            df = archive_data(df, df0,
                              sort_cols=['code', 'date'],
                              del_dup_cols=['code', 'date'],
                              sort_first=False,
                              csv_path=fpath,
                              csv_index=None)
            df.reset_index(drop=True, inplace=True)
            df_stocks[code] = df
            # loss = check_loss(df, code, trade_dates, logger=logger)
            # loss_stocks[code] = loss
    
    return data_all, loss_dates, df_stocks, loss_stocks


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from dramkit.iotools import get_last_change_time
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api(cfg.tushare_token2)
    # cfg.set_key_value('no_py_log', False)
    logger = gen_py_logger(sys.argv[0], config=cfg)
    
    trade_dates = load_trade_dates_tushare('SSE')
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_stocks_daily_stk_check(*args, **kwargs):
        return update_stocks_daily_stk_check(*args, **kwargs)
    
    
    fdir = find_target_dir('stocks/tushare/stocks_daily/',
                           root_dir=None, logger=logger)
    fpath = '{}astocks_daily_stk.csv'.format(fdir)
    tm_ = get_last_change_time(fpath)
    tm_dayorweek = dttools.get_dayofweek(tm_[:10])
    updated = False
    if (tm_dayorweek in [6, 7]) or \
                    (tm_dayorweek == 5 and tm_[11:16] >= '21:00'):
        updated = True
    dayofweek = dttools.get_dayofweek()
    now = datetime.datetime.now().strftime('%Y-%d-%m %H:%M:%S')[11:16]
    if dayofweek in [5, 6, 7] and not updated:
        split = True
    else:
        split = False
        
    
    dfs, losses = {}, {}
    data_all, loss_dates, df_stocks, loss_stocks = \
                            try_update_stocks_daily_stk_check(
                                save_path=None,
                                start_date='20220101',
                                root_dir=None,
                                trade_dates=trade_dates,
                                split=split,
                                ts_api=ts_api,
                                logger=logger)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









