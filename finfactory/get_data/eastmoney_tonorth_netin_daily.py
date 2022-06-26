# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dramkit import isnull, logger_show, load_csv
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_daily_data_is_new
from finfactory.utils.utils import check_date_loss


COLS_FINAL = ['date', '沪股通', '深股通', '北上资金']


def check_loss(data, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        # logger_show('北上资金净流入日数据有缺失日期：'+','.join(loss_dates),
        #             logger, 'warn')
        logger_show('北上资金净流入日数据缺失数：{}'.format(len(loss_dates)),
                    logger, 'warn')
    return loss_dates


def get_tonorth_netin():
    '''
    | 爬取东财北上资金净流入数据
    | https://data.eastmoney.com/hsgtcg/gzcglist.html
    '''
    url = ('https://push2his.eastmoney.com/api/qt/kamt.kline/get?'
           'fields1=f1,f3,f5&fields2=f51,f52&klt=101&lmt=50000')
    html = requests.get(url)
    bsObj = BeautifulSoup(html.content, 'lxml')    
    data = bsObj.find('p').get_text()
    data = data.replace('null', 'None')
    data = data.replace('true', 'True').replace('false', 'False')
    data = eval(data)['data']
    df_hk2sh = pd.DataFrame([x.split(',') for x in data['hk2sh']],
                            columns=['date', '沪股通']).set_index('date')
    df_hk2sz = pd.DataFrame([x.split(',') for x in data['hk2sz']],
                            columns=['date', '深股通']).set_index('date')
    df_s2n = pd.DataFrame([x.split(',') for x in data['s2n']],
                          columns=['date', '北上资金']).set_index('date')
    data = pd.concat((df_hk2sh, df_hk2sz, df_s2n), axis=1)
    data = data.astype(float)
    data.reset_index(inplace=True)
    # for col in ['沪股通', '深股通', '北上资金']:
    #     data[col] = data[col].astype(float) / 10000
    data = data[COLS_FINAL].copy()
    return data


def update_tonorth_netin_daily(df_exist=None, fpath=None, logger=None):
    '''
    | 从东方财富更新北上资金净流入日数据
    | https://data.eastmoney.com/hsgtcg/gzcglist.html
    '''
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath, encoding='gbk')
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
        
    df = get_tonorth_netin()    
    
    df_all = archive_data(df, df_exist,
                          sort_cols=['date'],
                          del_dup_cols=['date'],
                          sort_first=False,
                          csv_path=fpath,
                          csv_index=None,
                          csv_encoding='gbk')
        
    return df_all


def update_tonorth_netin_daily_check(save_path=None,
                                     root_dir=None,
                                     trade_dates=None,
                                     logger=None):
    '''从东方财富更新北上资金净流入日数据'''
    
    def _get_save_path(save_path):
        '''获取北上资金净流入日数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('tonorth_money/eastmoney/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'tonorth_money_netin.csv'
        return save_path
    
    save_path = _get_save_path(save_path)
    df_all = update_tonorth_netin_daily(df_exist=None,
                                  fpath=save_path,
                                  logger=logger)
    
    is_new, info = check_daily_data_is_new(df_all,
                   only_trade_day=True, trade_dates=trade_dates)
    if is_new:
        logger_show('北上资金净流入日数据更新完成！', logger, 'info')
    else:
        logger_show('北上资金净流入日数据更新未完成！数据最后日期：{}'.format(info[1]),
                    logger, 'warn')
    
    loss_dates = check_loss(df_all, trade_dates, logger)
    
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
    
    
    @try_repeat_run(cfg.try_get_eastmoney, logger=logger,
                    sleep_seconds=cfg.try_get_eastmoney_sleep)
    def try_update_tonorth_netin_daily_check(*args, **kwargs):
        return update_tonorth_netin_daily_check(*args, **kwargs)
    
    trade_dates = load_trade_dates_tushare('SSE')
    # trade_dates = None
    
    df, loss = try_update_tonorth_netin_daily_check(
                                        save_path=None,
                                        root_dir=None,
                                        trade_dates=trade_dates,
                                        logger=logger)
    
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    