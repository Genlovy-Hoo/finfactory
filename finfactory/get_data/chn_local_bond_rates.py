# -*- coding: utf-8 -*-

import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dramkit import load_csv, isnull, logger_show
from dramkit.datetimetools import date_reformat
from dramkit.datetimetools import get_dates_between
from dramkit.datetimetools import get_recent_workday_chncal
from finfactory.utils.utils import check_daily_data_is_new
from finfactory.load_his_data import find_target_dir


COLS_FINAL = ['日期', '1年', '2年', '3年', '5年', '7年',
              '10年', '15年', '20年', '30年']


def get_bond_yields_by_date(date, save_ori=False,
                            ori_save_path=None,
                            root_dir=None,
                            logger=None):
    '''
    | 从财政部网站爬取指定日期地方政府债收益率数据
    | date格式如: '2022-06-10'
    | https://yield.chinabond.com.cn/cbweb-czb-web/czb/moreInfo?locale=cn_ZH
    '''
    
    def _save_ori():
        if not save_ori or df_ori is None:
            return
        if isnull(ori_save_path):
            save_dir = find_target_dir('chn_local_bond_rates/data_ori/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + date + '.csv'
            df_ori.to_csv(save_path, index=None, encoding='gbk')
    
    def _get_data1():
        html = requests.get(url)
        bsObj = BeautifulSoup(html.content, 'html.parser')
        
        table = bsObj.find_all('table')[-1]
        df_ori = []
        for tr in table.find_all('tr'):
            line = [x.get_text() for x in tr.find_all('td')]
            df_ori.append(line)
            
        if len(df_ori) == 0:
            logger_show(f'{date}地方政府债收益率数据为空，返回None！', logger, 'warn')
            return None, None
            
        cols = df_ori[0]
        df_ori = df_ori[1:]
        for k in range(1, len(df_ori)):
            df_ori[k] = [date] + df_ori[k]
        df_ori = pd.DataFrame(df_ori, columns=cols)
        
        values = [date] + [eval(x) for x in list(df_ori['当日（%）'])]
        cols = ['日期'] + list(df_ori['期限'])    
        target = pd.DataFrame([values], columns=cols)
        target.drop_duplicates(subset=['日期'], keep='last', inplace=True)
        return target, df_ori
    
    def _get_data2():
        data = pd.read_html(url)
        if len(data) == 0:
            logger_show(f'{date}地方政府债收益率数据为空，返回None！', logger, 'warn')
            return None, None
        df_ori = data[-1]
        df_ori.columns = df_ori.iloc[0, :]
        df_ori = df_ori.iloc[1:, :].copy()
        target = df_ori[['期限', '当日（%）']].set_index('期限')
        target = target.transpose()
        target['日期'] = date
        target = target[COLS_FINAL]
        target.reset_index(drop=True, inplace=True)
        return target, df_ori
        
    date = date_reformat(date, '-')
    url = 'https://yield.chinabond.com.cn/cbweb-czb-web/czb/queryGjqxInfo?zblx=xy&workTime={}&locale=cn_ZH&qxmc=2'.format(date)
    logger_show(f'下载{date}地方政府债收益率数据...', logger)
    
    # target, df_ori = _get_data1()
    target, df_ori = _get_data2()
    
    _save_ori()
    
    return target, df_ori


def get_bond_yields_by_dates(dates, save_ori=False,
                             root_dir=None, logger=None):
    '''
    | 从财政部网站爬取指定多个日期地方政府债收益率数据
    | dates中日期格式如: '2022-06-10'
    '''
    data = []
    for date in dates:
        df, _ = get_bond_yields_by_date(date,
                                        save_ori=save_ori,
                                        ori_save_path=None,
                                        root_dir=root_dir,
                                        logger=logger)
        if df is None:
            df = pd.DataFrame(columns=COLS_FINAL)
        data.append(df)
    data = pd.concat(data, axis=0)
    data.set_index('日期', inplace=True)
    data.sort_index(ascending=True, inplace=True)
    data.dropna(how='all', inplace=True)
    data.reset_index(inplace=True)
    return data


def update_bond_yields_by_exist(data_exist, save_ori=False,
                                root_dir=None, logger=None):
    '''增量更新地方政府债收益率数据'''
    
    def _get_loss_dates(data, last_date):
        '''获取缺失日期'''
        exist_dates = data['日期'].unique().tolist()
        all_dates = get_dates_between(min(exist_dates), last_date,
                                      only_workday=True)
        loss_dates = [x for x in all_dates if x not in exist_dates]
        return loss_dates
    
    def _update_bond_yields(dates, data_exist):
        data = get_bond_yields_by_dates(dates, save_ori=save_ori,
                                        root_dir=root_dir, logger=logger)
        data = pd.concat((data_exist, data), axis=0)
        data.sort_values('日期', ascending=True, inplace=True)
        data.drop_duplicates('日期', keep='last', inplace=True)
        return data
    
    last_date = get_recent_workday_chncal(dirt='pre')    
    loss_dates = _get_loss_dates(data_exist, last_date)
    
    if len(loss_dates) == 0:
        logger_show('地方政府债收益率数据都已存在！', logger)
        return data_exist
    
    data = _update_bond_yields(loss_dates, data_exist)
    loss_dates = _get_loss_dates(data, last_date)
    while len(loss_dates) > 0 and max(loss_dates) < last_date:
        logger_show(f'地方政府债收益率数据补缺日期：{loss_dates}', logger)
        data = _update_bond_yields(loss_dates, data)
        loss_dates = _get_loss_dates(data, last_date)
        
    return data


def update_bond_yields(save_path=None, root_dir=None,
                       default_last_date='2022-02-24',
                       save_ori=True, logger=None):
    '''更新所有地方政府债收益率历史数据'''
    def _load_exist_bond_yields(save_path=None, root_dir=None):
        '''导入已存在地方政府债收益率历史数据'''
        if isnull(save_path):
            save_dir = find_target_dir('chn_local_bond_rates/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'chn_local_bond_rates.csv'
        data = load_csv(save_path, encoding='gbk', logger=logger)
        if not data is None:
            data.sort_values('日期', ascending=True, inplace=True)
            data.drop_duplicates(subset=['日期'], keep='last', inplace=True)
        return data, save_path
    
    data_exist, save_path = _load_exist_bond_yields(save_path, root_dir)
    no_exist = False
    if data_exist is None:
        no_exist = True
        if default_last_date is None:
            default_last_date = '2022-02-24'
        _tmp = date_reformat(default_last_date, '-')
        data_exist = pd.DataFrame([[_tmp]+[np.nan]*(len(COLS_FINAL)-1)],
                                  columns=COLS_FINAL)
    
    data = update_bond_yields_by_exist(data_exist,
                                       save_ori=save_ori,
                                       root_dir=root_dir,
                                       logger=logger)
    
    if no_exist:
        data = data.iloc[1:, :]
    data.to_csv(save_path, index=None, encoding='gbk')
        
    is_new, info = check_daily_data_is_new(data,
                                           date_col='日期',
                                           only_trade_day=False,
                                           only_workday=True,
                                           only_inweek=False)
    if is_new:
        logger_show('地方政府债收益率数据更新完成！', logger)
    else:
        logger_show(f'地方政府债收益率数据更新未完成！数据最后日期：{info[1]}',
                    logger, 'warn')
    
    return data


if __name__ == '__main__':
    import time
    from dramkit import simple_logger, close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg    
    strt_tm = time.time()
    
    
    logger = simple_logger('../../log/get_data_chn_local_bond_rates.log', 'a')
    
    
    @try_repeat_run(cfg.try_get_chn_bond_rates, logger=logger,
                    sleep_seconds=cfg.try_get_chn_bond_rates_sleep)
    def try_update_bond_yields(*args, **kwargs):
        return update_bond_yields(*args, **kwargs)
    
    
    df = try_update_bond_yields(save_path=None,
                                root_dir=None,
                                default_last_date=None,
                                save_ori=True,
                                logger=logger)
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    
    
    
    
    