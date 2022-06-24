# -*- coding: utf-8 -*-

import os
import urllib
import pandas as pd
import dramkit.datetimetools as dttools
from dramkit import isnull, logger_show, load_csv
from dramkit.other.othertools import archive_data
from dramkit.fintools.utils_chn import get_recent_trade_date
from dramkit.iotools import get_last_change_time
from finfactory.utils.utils import check_daily_data_is_new
from finfactory.utils.utils import check_date_loss
from finfactory.load_his_data import find_target_dir


COLS_FINAL = ['date', 'pe']


def check_loss(data, code, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        # logger_show('{}动态PE日数据有缺失日期：'.format(code)+','.join(loss_dates),
        #             logger, 'warn')
        logger_show('{}动态PE日数据缺失数：{}'.format(code, len(loss_dates)),
                    logger, 'warn')
    return loss_dates


def _load_js_data(fpath):
    '''导入js格式数据'''
    with open(fpath, 'r') as f:
        lines = f.readlines()
    data = lines[0].strip()
    data = data[:-1]
    data = data.split('=')[1]
    data = eval(data)
    data = [x.split('_') for x in data]
    data = [x for x in data if x[1] != '']
    data = pd.DataFrame(data, columns=['date', 'pe'])
    data['pe'] = data['pe'].apply(lambda x: eval(x))
    data['date'] = data['date'].apply(lambda x: dttools.date_reformat(x, '-'))
    return data[COLS_FINAL]


def get_index_pe_daily_cy(code, js_dir=None):
    '''获取当年的指数动态PE日数据'''
    if isnull(js_dir):
        js_dir_ = './'
    else:
        js_dir_ = js_dir
    js_path_cy = '{}dpe_{}_cy.js'.format(js_dir_, code)
    url = 'http://static.indexfunds.com.cn/market/ifs/{}_cy_dpe.js'.format(code)
    urllib.request.urlretrieve(url, js_path_cy)            
    data_cy = _load_js_data(js_path_cy)
    if isnull(js_dir):
        os.remove(js_path_cy)
    return data_cy[COLS_FINAL]


def get_index_pe_daily_his(code, js_dir=None):
    '''获取历史（当年以前）的指数动态PE日数据'''
    if isnull(js_dir):
        js_dir_ = './'
    else:
        js_dir_ = js_dir
    js_path_his = '{}dpe_{}_his.js'.format(js_dir_, code)
    last_trade_date = get_recent_trade_date(dirt='pre')
    if not os.path.exists(js_path_his) or \
       (get_last_change_time(js_path_his)[:7] < last_trade_date[:5]+'01'):
        url = 'http://static.indexfunds.com.cn/market/his/ifs/{}_all_dpe.js'.format(code)
        urllib.request.urlretrieve(url, js_path_his)
    data_his = _load_js_data(js_path_his)
    if isnull(js_dir):
        os.remove(js_path_his)
    return data_his[COLS_FINAL]


def get_index_pe_daily(code, js_dir=None):
    '''获取指数动态PE日数据'''
    df_cy = get_index_pe_daily_cy(code, js_dir)
    df_his = get_index_pe_daily_his(code, js_dir)
    df = pd.concat((df_his, df_cy), axis=0)
    df.sort_values('date', ascending=True, inplace=True)
    df.drop_duplicates(subset=['date'], inplace=True)
    return df[COLS_FINAL]


def update_index_pe_daily(code, df_exist=None, fpath=None,
                          js_dir=None, logger=None):
    '''更新指数动态PE数据'''
    
    def _to_update(df_exist):
        if isnull(df_exist) or df_exist.shape[0] < 1:
            return True
        date_max = df_exist['date'].max()
        last_trade_date = get_recent_trade_date(dirt='pre')
        if date_max >= last_trade_date:
            logger_show('{}动态PE最新数据已存在，不更新。', logger)
            return False
        return True
    
    if isnull(df_exist):
        if not isnull(fpath) and os.path.exists(fpath):
            df_exist = load_csv(fpath)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
    
    need_update = _to_update(df_exist)
    
    if not need_update:
        return df_exist
    
    logger_show('更新{}动态PE日数据 ...'.format(code), logger)    
    df = get_index_pe_daily(code, js_dir)
    
    df = archive_data(df, df_exist,
                      sort_cols=['date'],
                      del_dup_cols=['date'],
                      sort_first=False,
                      csv_path=fpath,
                      csv_index=None)
        
    return df


def update_index_pe_daily_check(code, save_path=None, js_dir=None,
                                root_dir=None, trade_dates=None,
                                logger=None):
    '''更新指数动态PE数据'''
    
    def _get_save_path(save_path, js_dir):
        '''获取指数动态PE日数据存放路径'''
        if isnull(js_dir):
            js_dir = find_target_dir('index/fundex/', root_dir=root_dir,
                                     make=True, logger=logger)
        if isnull(save_path):
            save_path = js_dir + 'dpe_{}.csv'.format(code)
        return save_path, js_dir
    
    save_path, js_dir = _get_save_path(save_path, js_dir)
    df_all = update_index_pe_daily(code,
                                   df_exist=None,
                                   fpath=save_path,
                                   js_dir=js_dir,
                                   logger=logger)
    
    is_new, info = check_daily_data_is_new(df_all,
                   only_trade_day=True, trade_dates=trade_dates)
    if is_new:
        logger_show('{}动态PE日数据更新完成！'.format(code), logger)
    else:
        logger_show('{}动态PE日数据更新未完成！数据最后日期：{}'.format(code, info[1]),
                    logger, 'warn')
    
    loss_dates = check_loss(df_all, code, trade_dates, logger)
    
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
    
    
    @try_repeat_run(cfg.try_get_fundex, logger=logger,
                    sleep_seconds=cfg.try_get_fundex_sleep)
    def try_update_index_pe_daily_check(*args, **kwargs):
        return update_index_pe_daily_check(*args, **kwargs)
    
    trade_dates_sh = load_trade_dates_tushare('SSE')
    trade_dates_sz = load_trade_dates_tushare('SZSE')
    
    codes = [
        '000905.SH', # 中证500
        '000300.SH', # 沪深300
        '000016.SH', # 上证50
        '399006.SZ', # 创业板指
        '000001.SH', # 上证指数
    ]
    for code in codes:
        if 'SH' in code:
            trade_dates = trade_dates_sh
        else:
            trade_dates = trade_dates_sz
        exec('''df_{}, loss_{} = try_update_index_pe_daily_check(
                                        code,
                                        save_path=None,
                                        js_dir=None,
                                        root_dir=None,
                                        trade_dates=trade_dates,
                                        logger=logger)
             '''.format(code[:6], code[:6])
            )
        time.sleep(2)
    
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    