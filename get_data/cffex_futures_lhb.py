# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import urllib.request
from dramkit import isnull, logger_show
from dramkit.iotools import read_lines
from dramkit.datetimetools import date_reformat
from dramkit.fintools.utils_chn import get_trade_dates
from finfactory.load_his_data import find_target_dir


CODE_START_END_DATES = {
    'IC': ['2015-04-16', None],
    'IF': ['2010-04-16', None],
    'IH': ['2015-04-16', None],
    'IO': ['2019-12-23', None],
    'T': ['2015-03-20', None],
    'TF': ['2013-09-06', None],
    'TS': ['2018-08-17', None]
}


def is_good_data(fpath, encoding=None, logger=None):
    '''判断是否为正确数据文件'''    
    if not os.path.exists(fpath):
        return False, '文件不存在'
    else:
        lines = read_lines(fpath, encoding=encoding, logger=logger)            
        for line in lines:
            if 'html' in line:
                return False, 'html'        
        return True, None


def get_save_path(code, date, root_dir=None, logger=None):
    save_dir = find_target_dir(
               'futures/cffex/lhb/{}/'.format(code),
               root_dir=root_dir, make=True, logger=logger)
    date = date_reformat(date, '')
    save_path = '{}{}{}.csv'.format(save_dir, code, date)
    return save_path


def download_cffex_lhb(code, date, save_path=None,
                       root_dir=None, logger=None):
    '''下载保存中金所期货龙虎榜数据'''
    date = date_reformat(date, '')
    month, day = date[:6], date[-2:]
    if isnull(save_path):
        save_path = get_save_path(code, date, root_dir, logger)
    url = 'http://www.cffex.com.cn/sj/ccpm/{}/{}/{}_1.csv'.format(
          month, day, code)
    logger_show('{}, {}, downloading...'.format(code, date), logger)
    try:
        urllib.request.urlretrieve(url, save_path)
        time.sleep(2)
    except:
        logger_show('{}, {}, download failed.'.format(code, date),
                    logger, 'error')
        
        
def download_cffex_lhb_all(code_start_end_dates=None,
                           root_dir=None, trade_dates=None,
                           check_last=True, check_all=False,
                           logger=None):
    '''
    下载所有中金所期货龙虎榜数据
    code_start_dates格式如：{'IC': '2015-04-16'}
    '''
    if isnull(code_start_end_dates):
        code_start_end_dates = CODE_START_END_DATES
    bad_files = []
    for code, start_end_date in code_start_end_dates.items():
        start_date, end_date = start_end_date
        dates = get_trade_dates(start_date, end_date,
                                trade_dates_df_path=trade_dates)
        dates.sort()
        ndates = len(dates)
        for k in range(ndates):
            date = dates[k]
            save_path = get_save_path(code, date, root_dir, logger)
            if check_all:
                is_good, info = is_good_data(save_path)
                if not is_good:
                    bad_files.append([code, date, int(is_good), info])
                    download_cffex_lhb(code, date, save_path,
                                       root_dir, logger)
            else:
                if k == ndates-1 and check_last:
                    is_good, info = is_good_data(save_path)
                    if not is_good:
                        bad_files.append([code, date, int(is_good), info])
                        download_cffex_lhb(code, date, save_path,
                                           root_dir, logger)
                else:
                    if not os.path.exists(save_path):
                        bad_files.append([code, date, 0, '文件不存在'])
                        download_cffex_lhb(code, date, save_path,
                                           root_dir, logger)
    bad_cols = ['code', 'date', 'is_good', 'info']
    bad_files = pd.DataFrame(bad_files, columns=bad_cols)
    return bad_files


def cffex_lhb_futures_check(code_start_end_dates=None,
                            root_dir=None, trade_dates=None,
                            del_bad=True, logger=None):
    if isnull(code_start_end_dates):
        code_start_end_dates = CODE_START_END_DATES
    df_check, paths_del = [], []
    for code, start_end_date in code_start_end_dates.items():
        start_date, end_date = start_end_date
        dates = get_trade_dates(start_date, end_date,
                                trade_dates_df_path=trade_dates)
        dates.sort()
        ndates = len(dates)
        for k in range(ndates):
            date = dates[k]
            save_path = get_save_path(code, date, root_dir, logger)
            is_good, info = is_good_data(save_path)
            if not is_good:
                df_check.append([code, date, int(is_good), info])
                if info != '文件不存在':
                    paths_del.append((save_path, info))
                    logger_show('无效文件: {}, {}'.format(code, date),
                                logger, 'warn')
                else:
                    logger_show('{}: {}, {}'.format(info, code, date),
                                logger, 'warn')
    if del_bad:
        for fpath, _ in paths_del:
            logger_show('删除无效文件: {}, {}'.format(code, date),
                        logger, 'info')
            os.remove(fpath)
    bad_cols = ['code', 'date', 'is_good', 'info']
    df_check = pd.DataFrame(df_check, columns=bad_cols)
    if df_check.shape[0] == 0:
        logger_show('中经社期货持仓龙虎榜数据没有无效文件。', logger)
    return df_check


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    
    strt_tm = time.time()
    logger = gen_py_logger(sys.argv[0])
    
    trade_dates = load_trade_dates_tushare('SSE')
    

    @try_repeat_run(cfg.try_get_cffex, logger=logger,
                    sleep_seconds=cfg.try_get_cffex_sleep)
    def try_download_cffex_lhb_all(*args, **kwargs):
        return download_cffex_lhb_all(*args, **kwargs)


    bad_files = try_download_cffex_lhb_all(
                            code_start_end_dates=None,
                            root_dir=None,
                            trade_dates=trade_dates,
                            check_all=False,
                            check_last=True,
                            logger=logger)
    
    bad_files = cffex_lhb_futures_check(logger=logger)
                
                
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    
    
    
    
