# -*- coding: utf-8 -*-

import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import dramkit.datetimetools as dttools
from dramkit import isnull, logger_show, load_csv
from dramkit.iotools import load_csvs
from dramkit.other.othertools import archive_data
from finfactory.fintools.utils_chn import get_trade_dates
from finfactory.load_his_data import find_target_dir
from finfactory.utils.utils import check_daily_data_is_new
from finfactory.utils.utils import check_date_loss


COLS_FINAL = ['code', 'name', 'date', 'open', 'high', 'low',
              'close', '成交量(万股)', '成交额(万元)', '涨跌幅(%)',
              '换手率(%)', '均价', '成交额占比(%)', 'PE', 'PB',
              '股息率(%)', '流通市值(万元)', '平均流通市值(万元)',
              'table']


def load_sw_daily_ori(root_dir=None, save_path=None):
    if isnull(save_path):
        save_dir = find_target_dir('sw/', root_dir=root_dir)
        save_path = save_dir + 'sw_daily.csv'
    ori_dir = find_target_dir('sw/daily_ori/', root_dir=root_dir)
    df = load_csvs(ori_dir,
                   kwargs_sort={'by': ['date', 'code']},
                   kwargs_drop_dup={'subset': ['date', 'code'],
                                    'keep': 'last'},
                   encoding='gbk')
    df.to_csv(save_path, index=None, encoding='gbk')
    return df


def check_loss(data, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('申万一级行业日数据有缺失日期：'+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def get_daily_info_by_date(date, save_ori=False,
                           ori_save_path=None, root_dir=None,
                           logger=None):
    '''
    | http://www.swsindex.com/idx0200.aspx?columnid=8838&type=Day
    | http://www.swsindex.com/idx0130.aspx?columnid=8838
    '''
    
    date = dttools.date_reformat(date, '-')
    
    url = 'http://www.swsindex.com/handler.aspx'
    headers = {
        'Host': 'www.swsindex.com',
        'Origin': 'http://www.swsindex.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
    }
    req_data = {
        'key': 'id',
        'orderby': 'swindexcode asc,BargainDate_1',
        'fieldlist': 'SwIndexCode,SwIndexName,BargainDate,CloseIndex,BargainAmount,Markup,TurnoverRate,PE,PB,MeanPrice,BargainSumRate,NegotiablesShareSum,NegotiablesShareSum2,DP,OpenIndex,MaxIndex,MinIndex,BargainSum',
        'pagecount': '1000000000'
    }    
    
    def _get_data(tablename):
        req_data['tablename'] = tablename
        req_data['where'] = "BargainDate>='{}' and BargainDate<='{}'".format(date, date)
        if tablename == 'V_Report':
            req_data['where'] += " and type='Day'"
        end_, df, p = False, [], 1
        while not end_:
            logger_show('{}, {}, page {} ...'.format(date, tablename, p),
                        logger)
            req_data['p'] = '{}'.format(p)
            r = requests.post(url, headers=headers, data=req_data)
            bsObj = BeautifulSoup(r.content, 'lxml')
            data = eval(bsObj.find('p').get_text())['root']
            if len(data) > 0:
                df += data
                p += 1
            else:
                end_ = True
        df = pd.DataFrame(df)
        df['table'] = tablename
        return df
    
    def _save_ori(df, date, save_ori,
                  ori_save_path, root_dir, logger):
        if not save_ori or isnull(df):
            return
        if isnull(ori_save_path):
            save_dir = find_target_dir('sw/daily_ori/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + date + '.csv'
            df.to_csv(save_path, index=None, encoding='gbk')
    
    df1 = _get_data('V_Report')
    df2 = _get_data('swindexhistory')
    
    df = pd.concat((df1, df2), axis=0)
    df.rename(columns={'SwIndexCode': 'code',
                       'SwIndexName': 'name',
                       'BargainDate': 'date',
                       'OpenIndex': 'open',
                       'MaxIndex': 'high',
                       'MinIndex': 'low',
                       'CloseIndex': 'close',
                       'BargainAmount': '成交量(万股)',
                       'Markup': '涨跌幅(%)',
                       'TurnoverRate': '换手率(%)',
                       'MeanPrice': '均价',
                       'BargainSumRate': '成交额占比(%)',
                       'NegotiablesShareSum': '流通市值(万元)',
                       'NegotiablesShareSum2': '平均流通市值(万元)',
                       'DP': '股息率(%)',                       
                       'BargainSum': '成交额(万元)'},
              inplace=True)
    if df.shape[0] > 0:
        df = df[COLS_FINAL]
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        _save_ori(df, date, save_ori,
                  ori_save_path, root_dir, logger)
    else:
        logger_show('{}获取到0条数据！'.format(date), logger, 'warn')
        df = pd.DataFrame(columns=COLS_FINAL)
    
    return df


def get_daily_info_by_dates(dates, save_ori=False,
                            root_dir=None, logger=None):
    '''
    指定多个日期，从申万网站爬取申万指数日数据
    '''
    data = []
    for k in range(len(dates)):
        date = dates[k]
        df = get_daily_info_by_date(date,
                                    save_ori=save_ori,
                                    ori_save_path=None,
                                    root_dir=root_dir,
                                    logger=logger)
        data.append(df)
        if k != len(dates)-1:
            logger_show('pausing...', logger)
            time.sleep(5)
    data = pd.concat(data, axis=0)
    data.sort_values(['date', 'code'], ascending=True, inplace=True)
    data.drop_duplicates(subset=['date', 'code'], keep='last', inplace=True)
    return data


def update_daily_info(df_exist=None, fpath=None,
                      save_ori=False, root_dir=None,
                      start_date=None, end_date=None,
                      trade_dates=None, logger=None):
    '''增量更新申万指数日数据'''
    
    def _get_loss_dates(df_exist, start_date):
        '''获取缺失日期'''
        if isnull(df_exist):
            fdir = find_target_dir('sw/daily_ori/', root_dir=root_dir,
                                   make=True, logger=logger)
            files = os.listdir(fdir)
            exist_dates = [x.replace('.csv', '') for x in files if len(x) == 14]
            if not isnull(fpath) and os.path.exists(fpath):
                df_exist = load_csv(fpath, encoding='gbk')
                if len(exist_dates) == 0:
                    if df_exist.shape[0] > 0:
                        exist_dates = df_exist['date'].unique().tolist()
        if len(exist_dates) > 0:
            first_date = min(exist_dates)
            all_dates = get_trade_dates(first_date, end_date,
                                        trade_dates)
            loss_dates = [x for x in all_dates if x not in exist_dates]
        else:
            if isnull(start_date):
                start_date = '2022-06-01'
                loss_dates = get_trade_dates(start_date, end_date,
                                             trade_dates)
        return loss_dates, df_exist
    
    loss_dates, df_exist = _get_loss_dates(df_exist, start_date)
    
    if len(loss_dates) == 0:
        logger_show('申万指数日数据都已存在！', logger)
        return df_exist
    
    data = get_daily_info_by_dates(loss_dates, save_ori=save_ori,
                                   root_dir=root_dir, logger=logger)
    if data.shape[0] == 0:
        return df_exist
    
    data = archive_data(data, df_exist,
                        sort_cols=['date', 'code'],
                        del_dup_cols=['date', 'code'],
                        sort_first=False,
                        csv_path=fpath,
                        csv_index=None,
                        csv_encoding='gbk')    
    
    return data


def update_daily_info_check(save_path=None, root_dir=None,
                            save_ori=True, start_date='2022-06-01',
                            trade_dates=None, logger=None):
    '''更新所有申万指数日数据'''
    
    def _get_save_path(save_path):
        '''获取申万指数日数据存放路径'''
        if isnull(save_path):
            save_dir = find_target_dir('sw/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'sw_daily.csv'
        return save_path
    
    save_path = _get_save_path(save_path)    
    data = update_daily_info(df_exist=None, fpath=save_path,
                             save_ori=save_ori, root_dir=root_dir,
                             start_date=start_date, end_date=None,
                             trade_dates=trade_dates, logger=logger)
        
    if data.shape[0] > 0:
        is_new, info = check_daily_data_is_new(data,
                                               date_col='date',
                                               trade_dates=trade_dates)
        if is_new:
            logger_show('申万指数日数据更新完成！', logger)
        else:
            logger_show(f'申万指数日数据更新未完成！数据最后日期：{info[1]}',
                        logger, 'warn')
        
        loss_dates = check_loss(data, trade_dates, logger)
    else:
        loss_dates = None
    
    return data, loss_dates


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    from finfactory.load_his_data import load_trade_dates_tushare
    strt_tm = time.time()
    
    
    logger = gen_py_logger(sys.argv[0])
    trade_dates = load_trade_dates_tushare('SSE')
    
    
    @try_repeat_run(cfg.try_get_sw, logger=logger,
                    sleep_seconds=cfg.try_get_sw_sleep)
    def try_update_daily_info_check(*args, **kwargs):
        return update_daily_info_check(*args, **kwargs)
    
    
    df = try_update_daily_info_check(save_path=None,
                                     root_dir=None,
                                     save_ori=True,
                                     start_date=None,
                                     trade_dates=trade_dates,
                                     logger=logger)
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    