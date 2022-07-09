# -*- coding: utf-8 -*-

import os
import pandas as pd
from dramkit import isnull, load_csv
from dramkit import logger_show
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_month_loss
from finfactory.utils.utils import get_tushare_api
from finfactory.load_his_data import find_target_dir


def get_cols_info(fpath=None, root_dir=None):
    '''读取字段信息'''
    if isnull(fpath):
        fdir = find_target_dir('chn_ppi/tushare/',
                               root_dir=root_dir)
        fpath = '{}cols.xlsx'.format(fdir)
    if not os.path.exists(fpath):
        return None, None, None, None
    df_cols = pd.read_excel(fpath)
    cols = list(df_cols['名称'])
    cols_chn = list(df_cols['描述'])
    fields = ','.join(list(df_cols['名称']))
    cols_map = df_cols.set_index('名称')['描述'].to_dict()
    return cols, cols_chn, fields, cols_map


def check_loss(df, logger=None):
    '''检查缺失'''
    loss_months = check_month_loss(df)
    if len(loss_months) > 0:
        # logger_show('PPI数据有缺失月份：'+','.join(loss_months),
        #             logger, 'warn')
        logger_show('PPI数据有缺失月份数：'+str(len(loss_months)),
                    logger, 'warn')
    return loss_months


def get_chn_ppi(start_month='197801', end_month=None,
                cols_path=None, root_dir=None, ts_api=None):
    '''
    tushare获取中国PPI数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_month):
        end_month = dttools.today_month('')
    cols, _, fields, _ = get_cols_info(cols_path, root_dir)
    df = ts_api.cn_ppi(start_m=start_month, end_m=end_month,
                       fields=fields)
    df['month'] = df['month'].apply(str)
    if not isnull(cols):
        df = df[cols]
    return df


def update_chn_ppi(df_exist=None, fpath=None,
                   start_month='197801', end_month=None,
                   cols_path=None, root_dir=None,
                   ts_api=None, logger=None):
    '''增量更新中国PPI数据'''
    
    def _load_exist_data(fpath):
        '''读取已存在的csv数据'''
        if isnull(fpath) or not os.path.exists(fpath):
            return None
        df = load_csv(fpath)
        df['month'] = df['month'].apply(lambda x: str(x)[:6])
        return df
    
    def _logger_show_last_month(df):
        logger_show(
            'PPI数据最新月份：{}'.format(df['month'].max()),
            logger, 'info')
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if isnull(end_month):
        end_month = dttools.today_month('')
        
    cols, _, _, _ = get_cols_info(cols_path, root_dir)
    
    if isnull(df_exist):
        df_exist = _load_exist_data(fpath)
    if not isnull(df_exist):
        if not isnull(cols):
            df_exist = df_exist[cols].copy()
        if df_exist.shape[0] > 0:
            start_month = df_exist['month'].max()
            start_month = str(start_month)[:6]
    
    if start_month >= end_month:
        logger_show('PPI最新数据已存在，不更新。', logger)
        _logger_show_last_month(df_exist)
        return df_exist
    
    logger_show('更新PPI数据, {}->{} ...'.format(start_month, end_month),
                logger, 'info')
    df = get_chn_ppi(start_month, end_month, cols_path,
                     root_dir, ts_api)
    if df.shape[0] == 0:
        logger_show('新获取0条记录，返回已存在数据。',
                    logger, 'warn')
        _logger_show_last_month(df_exist)
        return df_exist
    
    # 统一字段名
    if not isnull(cols):
        df = df[cols].copy()
    # 数据合并
    df_all = archive_data(df, df_exist,
                          sort_cols='month',
                          del_dup_cols='month',
                          sort_first=False,
                          csv_path=fpath,
                          csv_index=None)
    
    _logger_show_last_month(df_all)
    df_all.reset_index(drop=True, inplace=True)
    
    return df_all


def update_chn_ppi_check(save_path=None,
                         root_dir=None,
                         cols_path=None,
                         start_month='197801',
                         ts_api=None,
                         logger=None):
    '''
    更新中国PPI数据
    '''
    
    def _get_save_path(save_path):
        '''获取中国PPI数据存放路径'''
        if isnull(save_path):
            save_dir = find_target_dir('chn_ppi/tushare/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'chn_ppi.csv'
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_chn_ppi(df_exist=None,
                              fpath=save_path,
                              start_month=start_month,
                              end_month=None,
                              cols_path=cols_path,
                              root_dir=root_dir,
                              ts_api=ts_api,
                              logger=logger)
    
    loss_months = check_loss(data_all, logger=logger)
    
    return data_all, loss_months


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
    def try_update_chn_ppi_check(*args, **kwargs):
        return update_chn_ppi_check(*args, **kwargs)
    
    
    df, loss = try_update_chn_ppi_check(
                                save_path=None,
                                root_dir=None,
                                cols_path=None,
                                start_month='197801',
                                ts_api=ts_api,
                                logger=logger)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









