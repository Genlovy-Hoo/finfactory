# -*- coding: utf-8 -*-

import os
import pandas as pd
from dramkit import isnull
from dramkit import logger_show, load_csv
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import get_tushare_api
from finfactory.config import cfg


COLS_FINAL = ['code', 'date', 'price', 'volume',
              'amount', 'buyer', 'seller']

global TS_API_USED_TIMES
TS_API_USED_TIMES = 0


def check_loss(data, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data, only_workday=True,
                                 del_weekend=False)
    if len(loss_dates) > 0:
        # logger_show('大宗交易有缺失日期：'+','.join(loss_dates),
        #             logger, 'warn')
        logger_show('大宗交易缺失日期数：'+str(len(loss_dates)),
                    logger, 'warn')
    return loss_dates


def get_block_trades(start_date, end_date=None, ts_api=None):
    '''
    | tushare获取大宗交易数据
    | start_date和end_date格式：'20220630'
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    cols = {'ts_code': 'TS代码',
            'trade_date': '交易日期',
            'price': '成交价',
            'vol': '成交量（万股）',
            'amount': '成交金额',
            'buyer': '买方营业部',
            'seller': '卖方营业部'}
    cols_rename = {'ts_code': 'code', 'trade_date': 'date',
                   'vol': 'volume'}
    data = []
    global TS_API_USED_TIMES
    for date1, date2 in dttools.cut_date(start_date, end_date, 1):
        df = ts_api.block_trade(start_date=date1,
                                end_date=date2,
                                fields=','.join(list(cols.keys())))
        data.append(df)
        logger_show('{}, {}'.format(date1, df.shape), logger)
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_block_trade == 0:
            logger_show('{}, pausing...'.format(date1), logger)
            time.sleep(61)
    df = pd.concat(data, axis=0)
    df.rename(columns=cols_rename, inplace=True)
    df['date'] = df['date'].apply(lambda x: dttools.date_reformat(x, '-'))
    for col in ['price', 'volume', 'amount']:
        df[col] = df[col].astype(float).round(4)
    df = df[COLS_FINAL]
    df.sort_values(['date', 'code'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def update_block_trades(df_exist=None, fpath=None,
                        start_date='20070104', end_date=None,
                        ts_api=None, logger=None):
    '''增量更新大宗交易数据'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
        
    if isnull(start_date):
        start_date = '20070104'
    if isnull(end_date):
        end_date = dttools.today_date('')
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath, encoding='gbk')
            for col in ['price', 'volume', 'amount']:
                df_exist[col] = df_exist[col].astype(float).round(4)
    if not isnull(df_exist) and df_exist.shape[0] > 0:
        last_date = df_exist['date'].max()
        start_date = dttools.date_add_nday(last_date, 1)
    else:
        last_date = dttools.date_add_nday(start_date, -1)
            
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
    
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    last_date = dttools.date_reformat(last_date, '')
    
    if last_date >= end_date:
        logger_show('大宗交易最新数据已存在，不更新。', logger)
        return df_exist
    
    logger_show('更新大宗交易数据, {}->{} ...'.format(start_date, end_date),
                logger, 'info')
    data = []
    global TS_API_USED_TIMES
    for date1, date2 in dttools.cut_date(start_date, end_date, 1):
        df = get_block_trades(date1, date2, ts_api)
        data.append(df)
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_block_trade == 0:
            if len(data) > 1:
                data_ = pd.concat(data, axis=0)[COLS_FINAL]
                if data_.shape[0] > 0:
                    _ = archive_data(data_, df_exist,
                            sort_cols=['date', 'code'],
                            del_dup_cols=['date', 'code',
                                'price', 'volume', 'amount',
                                'buyer', 'seller'],
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None,
                            csv_encoding='gbk')            
            logger_show('{}, pausing...'.format(date1), logger)
            time.sleep(61)
    data = pd.concat(data, axis=0)
    if data.shape[0] == 0:
        logger_show('新获取0条记录，返回已存在数据。',
                    logger, 'warn')
        return df_exist
    
    # 统一字段名    
    data = data[COLS_FINAL].copy()
    # 数据合并
    data_all = archive_data(data, df_exist,
                            sort_cols=['date', 'code'],
                            del_dup_cols=['date', 'code',
                                'price', 'volume', 'amount',
                                'buyer', 'seller'],
                            sort_first=False,
                            csv_path=fpath,
                            csv_index=None,
                            csv_encoding='gbk')
    data_all.reset_index(drop=True, inplace=True)
    
    return data_all


def update_block_trades_check(save_path=None,
                              root_dir=None,
                              start_date='20070104',
                              ts_api=None,
                              logger=None):
    '''
    更新大宗交易行情数据
    '''
    
    def _get_save_path(save_path):
        '''获取大宗交易历史数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('block_trades/tushare/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'block_trades.csv'
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_block_trades(df_exist=None,
                                   fpath=save_path,
                                   start_date=start_date,
                                   end_date=None,
                                   ts_api=ts_api,
                                   logger=logger)
    
    logger_show('大宗交易数据最新日期: {}。'.format(data_all['date'].max()), logger)
    
    loss_dates = check_loss(data_all, logger=logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_block_trades_check(*args, **kwargs):
        return update_block_trades_check(*args, **kwargs)
    
    
    df, loss = try_update_block_trades_check(
                                    save_path=None,
                                    root_dir=None,
                                    start_date='20070104',
                                    ts_api=ts_api,
                                    logger=logger)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









