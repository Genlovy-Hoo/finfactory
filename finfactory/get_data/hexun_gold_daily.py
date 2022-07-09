# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dramkit import isnull
from dramkit import logger_show, load_csv
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_date_loss


COLS_FINAL = ['date', 'open', 'high', 'low', 'close',
              'pre_close', 'volume', 'amount']


def check_loss(data, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data, only_workday=False,
                                 del_weekend=True)
    if len(loss_dates) > 0:
        # logger_show('黄金现货日线数据有缺失日期：'+','.join(loss_dates),
        #             logger, 'warn')
        logger_show('黄金现货日线数据缺失数：{}'.format(len(loss_dates)),
                    logger, 'warn')
    return loss_dates


def _get_str_data(str_data):
    '''字符串数据提取'''
    data = eval(str_data.strip()[:-1])
    df = pd.DataFrame(data['Data'][0],
                      columns=[list(x.keys())[0] for x in data['KLine']])
    df['date'] = df['Time'].apply(lambda x: str(x)[:8])
    df['date'] = df['date'].apply(dttools.date_reformat)
    df = df.reindex(columns=['date', 'Open', 'High', 'Low', 'Close',
                             'LastClose', 'Volume', 'Amount'])
    df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low',
                       'Close': 'close', 'LastClose': 'pre_close',
                       'Volume': 'volume', 'Amount': 'amount'}, inplace=True)
    for col in ['open', 'high', 'low', 'close', 'pre_close']:
        df[col] = df[col] / 10000
    return df


def get_gold_daily(start_date='1983-01-05', end_date=None, logger=None):
    '''
    | 和讯网爬取黄金现货日K线数据
    | http://gold.hexun.com/hjxh/
    '''
    if isnull(end_date):
        end_date = dttools.today_date()
    if isnull(start_date):
        start_date = '1983-01-05'
    start_date = dttools.date_reformat(start_date, '-')
    end_date = dttools.date_reformat(end_date, '-')
    dates = dttools.cut_date(start_date, end_date, 800)
    data = []
    for dt1, dt2 in dates:
        dt = dttools.date_reformat(dt1, '') + '080000'
        n = dttools.diff_days_date(dt2, dt1) + 2
        logger_show('get hexun gold daily, {}'.format(dt1), logger)
        url = 'http://webforex.hermes.hexun.com/forex/kline?code=FOREXXAUUSD&start={}&number={}&type=5'.format(dt, n)
        html = requests.get(url,
                            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'})
        bsObj = BeautifulSoup(html.content, 'lxml')
        df = _get_str_data(bsObj.find('p').get_text())
        data.append(df)
    data = pd.concat(data, axis=0)
    data.sort_values('date', ascending=True, inplace=True)
    data.drop_duplicates(subset=['date'], keep='last', inplace=True)
    return data


def update_gold_daily(df_exist=None,
                      fpath=None,
                      start_date='1983-01-05',
                      end_date=None,
                      logger=None):
    '''增量更新黄金现货日线数据'''
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
        if df_exist.shape[0] > 0:
            last_date = df_exist['date'].max()
            start_date = dttools.date_add_nday(last_date, -10)
            
    data = get_gold_daily(start_date, end_date, logger)
    
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


def update_gold_daily_check(save_path=None,
                            root_dir=None,
                            start_date='1983-01-05',
                            logger=None):
    '''
    更新黄金现货日线行情数据
    '''
    
    def _get_save_path(save_path):
        '''获取黄金现货日线历史数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('gold/hexun/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'gold_usd_daily.csv'
        return save_path
    
    save_path = _get_save_path(save_path)    
    data_all = update_gold_daily(df_exist=None,
                                 fpath=save_path,
                                 start_date=start_date,
                                 end_date=None,
                                 logger=logger)
    
    loss_dates = check_loss(data_all, logger=logger)
    
    return data_all, loss_dates


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_hexun, logger=logger,
                    sleep_seconds=cfg.try_get_hexun_sleep)
    def try_update_gold_daily_check(*args, **kwargs):
        return update_gold_daily_check(*args, **kwargs)
    
    
    df, loss = try_update_gold_daily_check(save_path=None,
                                           root_dir=None,
                                           start_date=None,
                                           logger=logger)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









