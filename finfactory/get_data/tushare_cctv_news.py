# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from dramkit import logger_show, isnull
from dramkit.datetimetools import today_date
from dramkit.datetimetools import date_reformat
from dramkit.datetimetools import get_dates_between
from finfactory.load_his_data import find_target_dir
from finfactory.utils.utils import get_tushare_api
from finfactory.config import cfg


COLS_FINAL = ['date', 'title', 'content']

global TS_API_USED_TIMES
TS_API_USED_TIMES = 0


def check_loss(save_dir=None, root_dir=None, logger=None):
    '''检查缺失'''
    if isnull(save_dir):
        save_dir = find_target_dir('cctv_news/tushare/',
                                   root_dir=root_dir)
    if not os.path.exists(save_dir):
        logger_show('新闻联播文本存放路径不存在！', logger, 'warn')
        return None, None
    files = os.listdir(save_dir)
    if len(files) == 0:
        logger_show('新闻联播文本数据为空！', logger, 'warn')
        return None, None
    dates = [x[:8] for x in files if x.endswith('.csv') and len(x) == 12]
    dates_all = get_dates_between(min(dates), max(dates),
                                  keep1=True, keep2=True)
    loss_dates = [x for x in dates_all if x not in dates]
    if len(loss_dates) > 0:
        logger_show('新闻联播文本数据有缺失日期：'+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates, dates


def get_cctv_news_by_date(date=None, fpath=None, 
                          ts_api=None, logger=None):
    '''
    tushare获取新闻联播文本数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(date):
        date = today_date('')
    date = date_reformat(date, '')
    df = ts_api.cctv_news(date=date)
    if df.shape[0] == 0:
        logger_show('{}新闻联播未取到数据，可能缺失！'.format(date),
                    logger, 'warn')
    df = df[COLS_FINAL]
    if not isnull(fpath) and df.shape[0] > 0:
        df.to_csv(fpath, index=None)
    return df


def get_cctv_news_by_dates(dates, save_dir=None, 
                           ts_api=None, logger=None):
    '''
    tushare获取新闻联播文本数据
    '''
    data = []
    global TS_API_USED_TIMES
    for date in dates:
        logger_show('{}...'.format(date), logger)
        if isnull(save_dir):
            fpath = None
        else:
            date = date_reformat(date, '')
            fpath = os.path.join(save_dir, date+'.csv')
        df = get_cctv_news_by_date(date, fpath, ts_api, logger)
        data.append(df)
        TS_API_USED_TIMES += 1
        if TS_API_USED_TIMES % cfg.ts_1min_cctv_news == 0:
            logger_show('pausing...', logger)
            time.sleep(61)
    data = pd.concat(data, axis=0)[COLS_FINAL]
    return data


def get_cctv_news(start_date, end_date, save_dir=None,
                  ts_api=None, logger=None):
    dates = get_dates_between(start_date, end_date,
                              keep1=True, keep2=True)
    return get_cctv_news_by_dates(dates, save_dir, ts_api, logger)


def update_cctv_news(save_dir=None, root_dir=None,
                     start_date='20060615', end_date=None,
                     get_his_loss=False, ts_api=None,
                     logger=None):
    '''更新新闻联播文本数据'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
        
    if isnull(save_dir):
        save_dir = find_target_dir('cctv_news/tushare/',
                                   root_dir=root_dir)
        
    if isnull(end_date):
        end_date = today_date('')
    start_date = date_reformat(start_date, '')
    end_date = date_reformat(end_date, '')
    dates_all = get_dates_between(start_date, end_date,
                                  keep1=True, keep2=True)
    loss_dates, dates = check_loss(save_dir, root_dir, False)
    last_date = max(dates)
    dates = [x for x in dates_all if x not in dates]
    
    if not get_his_loss:
        dates = [x for x in dates if x >= last_date]
        
    if len(dates) == 0:
        logger_show('新闻联播最新数据已存在，不更新。', logger)
        return None, loss_dates
    
    dates.sort()
    logger_show('更新新闻联播文本, {}->{} ...'.format(dates[0], dates[-1]),
                logger, 'info')
    
    data = get_cctv_news_by_dates(dates, save_dir, 
                                  ts_api, logger=logger)
    
    loss_dates, _ = check_loss(save_dir, root_dir, logger)
    
    return data, loss_dates


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api(cfg.tushare_token2)
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_cctv_news(*args, **kwargs):
        return update_cctv_news(*args, **kwargs)
    
    
    df, loss = try_update_cctv_news(save_dir=None,
                                    root_dir=None,
                                    start_date='20060615',
                                    end_date=None,
                                    get_his_loss=False,
                                    ts_api=ts_api,
                                    logger=logger)
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    

    
    
    
    