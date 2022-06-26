# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dramkit import load_csv, isnull, logger_show
from dramkit.datetimetools import date_reformat
from dramkit.datetimetools import get_dates_between
from dramkit.datetimetools import get_recent_workday_chncal
from finfactory.utils.utils import check_daily_data_is_new
from finfactory.utils.utils import check_date_loss
from finfactory.load_his_data import find_target_dir


COLS_FINAL = ['日期', '1年', '2年', '3年', '5年', '7年',
              '10年', '15年', '20年', '30年']


def check_loss(data, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data, only_workday=True)
    if len(loss_dates) > 0:
        logger_show('地方政府债收益率数据有缺失日期：'+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


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
            save_dir = find_target_dir('chn_bonds/local/data_ori/',
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
    从财政部网站爬取指定多个日期地方政府债收益率数据
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


def update_bond_yields(df_exist=None, fpath=None,
                       save_ori=False, root_dir=None,
                       start_date=None, end_date=None,
                       logger=None):
    '''增量更新地方政府债收益率数据'''
    
    def _get_loss_dates(data, end_date):
        '''获取缺失日期'''
        exist_dates = data['日期'].unique().tolist()
        all_dates = get_dates_between(min(exist_dates), end_date,
                                      only_workday=True)
        loss_dates = [x for x in all_dates if x not in exist_dates]
        return loss_dates
    
    def _update_bond_yields(dates, df_exist):
        data = get_bond_yields_by_dates(dates, save_ori=save_ori,
                                        root_dir=root_dir, logger=logger)
        data = pd.concat((df_exist, data), axis=0)
        data.sort_values('日期', ascending=True, inplace=True)
        data.drop_duplicates('日期', keep='last', inplace=True)
        return data
           
    if df_exist is None:
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath, encoding='gbk', logger=logger)
            df_exist.sort_values('日期', ascending=True, inplace=True)
            df_exist.drop_duplicates(subset=['日期'], keep='last', inplace=True)
        else:
            if isnull(start_date):
                start_date = '2022-02-24'
            start_date = get_recent_workday_chncal(start_date, 'pre')
            df_exist, _ = get_bond_yields_by_date(
                                            start_date,
                                            save_ori=save_ori,
                                            ori_save_path=None,
                                            root_dir=root_dir,
                                            logger=logger)
    
    if isnull(end_date):
        end_date = get_recent_workday_chncal(dirt='pre')    
    loss_dates = _get_loss_dates(df_exist, end_date)
    
    if len(loss_dates) == 0:
        logger_show('地方政府债收益率数据都已存在！', logger)
        # if not isnull(fpath):
        #     df_exist.to_csv(fpath, index=None, encoding='gbk')
        return df_exist
    
    data = _update_bond_yields(loss_dates, df_exist)
    loss_dates = _get_loss_dates(data, end_date)
    while len(loss_dates) > 0 and max(loss_dates) < end_date:
        logger_show(f'地方政府债收益率数据补缺日期：{loss_dates}', logger)
        data = _update_bond_yields(loss_dates, data)
        loss_dates = _get_loss_dates(data, end_date)
        
    if not isnull(fpath):
        data.to_csv(fpath, index=None, encoding='gbk')
        
    return data


def update_bond_yields_check(save_path=None, root_dir=None,
                             start_date='2022-02-24',
                             save_ori=True, logger=None):
    '''更新所有地方政府债收益率历史数据'''
    
    def _get_save_path(save_path):
        '''获取地方政府债收益率历史数据存放路径'''
        if isnull(save_path):
            save_dir = find_target_dir('chn_bonds/local/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'chn_local_bond_rates.csv'
        return save_path
    
    save_path = _get_save_path(save_path)    
    data = update_bond_yields(df_exist=None, fpath=save_path,
                              save_ori=save_ori, root_dir=root_dir,
                              start_date=start_date, end_date=None,
                              logger=logger)
        
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

    loss_dates = check_loss(data, logger)
    
    return data, loss_dates


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_chn_bond_rates, logger=logger,
                    sleep_seconds=cfg.try_get_chn_bond_rates_sleep)
    def try_update_bond_yields_check(*args, **kwargs):
        return update_bond_yields_check(*args, **kwargs)
    
    
    df, loss = try_update_bond_yields_check(save_path=None,
                                            root_dir=None,
                                            start_date=None,
                                            save_ori=True,
                                            logger=logger)
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    
    
    
    
    