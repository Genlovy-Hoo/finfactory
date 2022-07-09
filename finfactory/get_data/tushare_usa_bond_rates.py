# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from dramkit import isnull
from dramkit import logger_show, load_csv
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_date_loss
from finfactory.utils.utils import get_tushare_api
from finfactory.config import cfg


COLS_FINAL = ['date', 'm1', 'm2', 'm3', 'm6', 'y1', 'y2',
              'y3', 'y5', 'y7', 'y10', 'y20', 'y30']

global TS_API_USED_TIMES
TS_API_USED_TIMES = 0


def check_loss(data, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data, only_workday=False,
                                 del_weekend=True)
    if len(loss_dates) > 0:
        # logger_show('美国债收益率日数据有缺失日期：'+','.join(loss_dates),
        #             logger, 'warn')
        logger_show('美国债收益率日数据缺失数：'+str(len(loss_dates)),
                    logger, 'warn')
    return loss_dates


def get_usa_bonds(start_date, end_date=None, ts_api=None):
    '''
    tushare获取美国债收益率日数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_date):
        end_date = dttools.today_date('')
    start_date = dttools.date_reformat(start_date, '')
    end_date = dttools.date_reformat(end_date, '')
    cols = {'date': 'date', 'm1': '1月期', 'm2': '2月期',
            'm3': '3月期', 'm6': '6月期', 'y1': '1年期',
            'y2': '2年期', 'y3': '3年期', 'y5': '5年期',
            'y7': '7年期', 'y10': '10年期', 'y20': '20年期',
            'y30': '30年期'}
    data = []
    for date1, date2 in dttools.cut_date(start_date, end_date, 1800):
        df = ts_api.us_tycr(start_date=date1,
                            end_date=date2,
                            fields=','.join(list(cols.keys())))
        data.append(df)
        global TS_API_USED_TIMES
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_us_tycr == 0:
            logger_show('{}, pausing...'.format(date1), logger)
            time.sleep(61)
    df = pd.concat(data, axis=0)
    df['date'] = df['date'].apply(lambda x: dttools.date_reformat(x, '-'))
    df = df[COLS_FINAL]
    df.sort_values('date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def update_usa_bonds(df_exist=None, fpath=None,
                     start_date='19900101', end_date=None,
                     ts_api=None, logger=None):
    '''增量更新美国债收益率日数据'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
        
    if isnull(start_date):
        start_date = '19900101'
    if isnull(end_date):
        end_date = dttools.today_date('')
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath)
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
        logger_show('美国债收益率最新数据已存在，不更新。', logger)
        return df_exist
    
    logger_show('更新美国债收益率数据, {}->{} ...'.format(start_date, end_date),
                logger, 'info')
    data = get_usa_bonds(start_date, end_date, ts_api)
    if data.shape[0] == 0:
        logger_show('新获取0条记录，返回已存在数据。',
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


def update_usa_bonds_check(save_path=None,
                           root_dir=None,
                           start_date='19900101',
                           ts_api=None,
                           logger=None):
    '''
    更新美国债收益率日数据
    '''
    
    def _get_save_path(save_path):
        '''获取美国债收益率历史数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('usa_bonds/tushare/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'usa_bond_rates.csv'
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_usa_bonds(df_exist=None,
                                fpath=save_path,
                                start_date=start_date,
                                end_date=None,
                                ts_api=ts_api,
                                logger=logger)
    
    logger_show('美国债收益率数据最新日期: {}。'.format(data_all['date'].max()), logger)
    
    loss_dates = check_loss(data_all, logger=logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_usa_bonds_check(*args, **kwargs):
        return update_usa_bonds_check(*args, **kwargs)
    
    
    
    df, loss = try_update_usa_bonds_check(
                                save_path=None,
                                root_dir=None,
                                start_date='19900101',
                                ts_api=ts_api,
                                logger=logger)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









