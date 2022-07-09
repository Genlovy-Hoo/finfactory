# -*- coding: utf-8 -*-

import os
from dramkit import isnull, load_csv
from dramkit import logger_show
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_quarter_loss
from finfactory.utils.utils import get_tushare_api


COLS_FINAL = ['quarter', 'gdp', 'gdp_yoy', 'pi', 'pi_yoy',
              'si', 'si_yoy', 'ti', 'ti_yoy']


def check_loss(df, logger=None):
    '''检查缺失'''
    loss_quarters = check_quarter_loss(df)
    if len(loss_quarters) > 0:
        # logger_show('GDP数据有缺失季度数：'+','.join(loss_quarters),
        #             logger, 'warn')
        logger_show('GDP数据有缺失季度数：'+str(len(loss_quarters)),
                    logger, 'warn')
    return loss_quarters


def get_chn_gdp(start_quarter='1950Q1',
                end_quarter=None,
                ts_api=None):
    '''
    tushare获取中国GDP数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_quarter):
        end_quarter = dttools.today_quarter()
    cols = {'quarter': '季度',
            'gdp': 'GDP累计值（亿元）',
            'gdp_yoy': '当季同比增速（%）',
            'pi': '第一产业累计值（亿元）',
            'pi_yoy': '第一产业同比增速（%）',
            'si': '第二产业累计值（亿元）',
            'si_yoy': '第二产业同比增速（%）',
            'ti': '第三产业累计值（亿元）',
            'ti_yoy': '第三产业同比增速（%）'}
    df = ts_api.cn_gdp(start_q=start_quarter,
                       end_q=end_quarter,
                       fields=','.join(list(cols.keys())))
    df = df[COLS_FINAL]
    return df


def update_chn_gdp(df_exist=None, fpath=None,
                   start_quarter='1950Q1', end_quarter=None,
                   ts_api=None, logger=None):
    '''增量更新中国GDP数据'''
    
    def _load_exist_data(fpath):
        '''读取已存在的csv数据'''
        if isnull(fpath) or not os.path.exists(fpath):
            return None
        df = load_csv(fpath)
        return df
    
    def _logger_show_last_quarter(df):
        logger_show(
            'GDP数据最新季度：{}'.format(df['quarter'].max()),
            logger, 'info')
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if isnull(end_quarter):
        end_quarter = dttools.today_quarter()
    
    if isnull(df_exist):
        df_exist = _load_exist_data(fpath)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
        if df_exist.shape[0] > 0:
            start_quarter = df_exist['quarter'].max()
    
    if start_quarter >= end_quarter:
        logger_show('GDP最新数据已存在，不更新。', logger)
        _logger_show_last_quarter(df_exist)
        return df_exist
    
    logger_show('更新GDP数据, {}->{} ...'.format(start_quarter, end_quarter),
                logger, 'info')
    df = get_chn_gdp(start_quarter, end_quarter, ts_api)
    if df.shape[0] == 0:
        logger_show('新获取0条记录，返回已存在数据。',
                    logger, 'warn')
        _logger_show_last_quarter(df_exist)
        return df_exist
    
    # 统一字段名    
    df = df[COLS_FINAL].copy()
    # 数据合并
    df_all = archive_data(df, df_exist,
                          sort_cols='quarter',
                          del_dup_cols='quarter',
                          sort_first=False,
                          csv_path=fpath,
                          csv_index=None)
    
    _logger_show_last_quarter(df_all)
    df_all.reset_index(drop=True, inplace=True)
    
    return df_all


def update_chn_gdp_check(save_path=None,
                         root_dir=None,
                         start_quarter='1950Q1',
                         ts_api=None,
                         logger=None):
    '''
    更新中国GDP数据
    '''
    
    def _get_save_path(save_path):
        '''获取中国GDP数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('chn_gdp/tushare/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'chn_gdp.csv'
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_chn_gdp(df_exist=None,
                              fpath=save_path,
                              start_quarter=start_quarter,
                              end_quarter=None,
                              ts_api=ts_api,
                              logger=logger)
    
    loss_quarters = check_loss(data_all, logger=logger)
    loss_quarters = []
    
    return data_all, loss_quarters


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_chn_gdp_check(*args, **kwargs):
        return update_chn_gdp_check(*args, **kwargs)
    
    
    df, loss = try_update_chn_gdp_check(
                                save_path=None,
                                root_dir=None,
                                start_quarter='1950Q1',
                                ts_api=ts_api,
                                logger=logger)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









