# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dramkit import isnull, logger_show, load_csv
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_daily_data_is_new
from finfactory.utils.utils import check_date_loss


COLS_FINAL = ['date', 'pe', 'close']


def check_loss(data, code, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        # logger_show('{}PE日数据有缺失日期：'.format(code)+','.join(loss_dates),
        #             logger, 'warn')
        logger_show('{}PE日数据缺失数：{}'.format(code, len(loss_dates)),
                    logger, 'warn')
    return loss_dates


def get_index_pe(eniu_code):
    '''
    | 爬取亿牛网指数PE估值数据
    | eniu_code格式如：sz399300、sh000016
    | https://eniu.com/gu/sz399300
    '''
    url = 'https://eniu.com/chart/peindex/{}/t/all'.format(eniu_code)
    html = requests.get(url)
    bsObj = BeautifulSoup(html.content, 'lxml')    
    data = eval(bsObj.find('p').get_text())
    data = pd.DataFrame(data)
    data.sort_values('date', ascending=True, inplace=True)
    data = data[COLS_FINAL].copy()
    return data


def update_index_pe(eniu_code, df_exist=None,
                    fpath=None, logger=None):
    '''
    | 从亿牛网爬取更新指数PE估值日数据
    | eniu_code格式如：sz399300、sh000016
    | https://eniu.com/
    '''
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
        
    df = get_index_pe(eniu_code)    
    
    df_all = archive_data(df, df_exist,
                          sort_cols=['date'],
                          del_dup_cols=['date'],
                          sort_first=False,
                          csv_path=fpath,
                          csv_index=None)
    df_all.reset_index(drop=True, inplace=True)
        
    return df_all


def update_index_pe_check(eniu_code, 
                          save_path=None,
                          root_dir=None,
                          trade_dates=None,
                          logger=None):
    '''从亿牛网爬取更新指数PE估值日数据'''
    
    def _get_save_path(save_path):
        '''获取指数PE估值日数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('index/eniu/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + '{}_pe_daily.csv'.format(eniu_code)
        return save_path
    
    save_path = _get_save_path(save_path)
    df_all = update_index_pe(eniu_code,
                             df_exist=None,
                             fpath=save_path,
                             logger=logger)
    
    is_new, info = check_daily_data_is_new(df_all,
                   only_trade_day=True, trade_dates=trade_dates)
    if is_new:
        logger_show('{}指数PE日数据更新完成！'.format(eniu_code),
                    logger, 'info')
    else:
        logger_show('{}指数PE日数据更新未完成！数据最后日期：{}'.format(eniu_code, info[1]),
                    logger, 'warn')
    
    loss_dates = check_loss(df_all, eniu_code, trade_dates, logger)
    
    df_all.sort_values('date', ascending=False, inplace=True)
    return df_all, loss_dates


if __name__ == '__main__': 
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_eniu, logger=logger,
                    sleep_seconds=cfg.try_get_eniu_sleep)
    def try_update_index_pe_check(*args, **kwargs):
        return update_index_pe_check(*args, **kwargs)
    
    trade_dates_sh = load_trade_dates_tushare('SSE')
    trade_dates_sz = load_trade_dates_tushare('SZSE')
    
    eniu_codes = [
        'sz399300', # 沪深300
        'sh000905', # 中证500        
        'sh000016', # 上证50
        'sh000001', # 上证指数
        'sh000688', # 科创50
        'sz399102', # 创业板综
        ]
    
    dfs, losses = {}, {}
    for eniu_code in eniu_codes:
        if 'sh' in eniu_code:
            trade_dates = trade_dates_sh
        else:
            trade_dates = trade_dates_sz
        exec('''dfs['{}'], losses['{}'] = try_update_index_pe_check(
                                    eniu_code, 
                                    save_path=None,
                                    root_dir=None,
                                    trade_dates=trade_dates,
                                    logger=logger)
             '''.format(eniu_code, eniu_code)
            )
    
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    