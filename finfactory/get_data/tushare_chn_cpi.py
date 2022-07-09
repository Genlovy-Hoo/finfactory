# -*- coding: utf-8 -*-

import os
from dramkit import isnull, load_csv
from dramkit import logger_show
import dramkit.datetimetools as dttools
from dramkit.other.othertools import archive_data
from finfactory.utils.utils import check_month_loss
from finfactory.utils.utils import get_tushare_api


COLS_FINAL = ['month', 'nt_val', 'nt_yoy', 'nt_mom',
              'nt_accu', 'town_val', 'town_yoy',
              'town_mom', 'town_accu', 'cnt_val',
              'cnt_yoy', 'cnt_mom', 'cnt_accu']


def check_loss(df, logger=None):
    '''检查缺失'''
    loss_months = check_month_loss(df)
    if len(loss_months) > 0:
        # logger_show('CPI数据有缺失月份：'+','.join(loss_months),
        #             logger, 'warn')
        logger_show('CPI数据有缺失月份数：'+str(len(loss_months)),
                    logger, 'warn')
    return loss_months


def get_chn_cpi(start_month='195101',
                end_month=None,
                ts_api=None):
    '''
    tushare获取中国CPI数据
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    if isnull(end_month):
        end_month = dttools.today_month('')
    cols = {'month': '月份', 'nt_val': '全国当月值',
            'nt_yoy': '全国同比（%）', 'nt_mom': '全国环比（%）',
            'nt_accu': '全国累计值', 'town_val': '城市当月值',
            'town_yoy': '城市同比（%）', 'town_mom': '城市环比（%）',
            'town_accu': '城市累计值', 'cnt_val': '农村当月值',
            'cnt_yoy': '农村同比（%）', 'cnt_mom': '农村环比（%）',
            'cnt_accu': '农村累计值'}
    df = ts_api.cn_cpi(start_m=start_month, end_m=end_month,
                       fields=','.join(list(cols.keys())))
    df['month'] = df['month'].apply(str)
    df = df[COLS_FINAL]
    return df


def update_chn_cpi(df_exist=None, fpath=None,
                   start_month='195101', end_month=None,
                   ts_api=None, logger=None):
    '''增量更新中国CPI数据'''
    
    def _load_exist_data(fpath):
        '''读取已存在的csv数据'''
        if isnull(fpath) or not os.path.exists(fpath):
            return None
        df = load_csv(fpath)
        df['month'] = df['month'].apply(lambda x: str(x)[:6])
        return df
    
    def _logger_show_last_month(df):
        logger_show(
            'CPI数据最新月份：{}'.format(df['month'].max()),
            logger, 'info')
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if isnull(end_month):
        end_month = dttools.today_month('')
    
    if isnull(df_exist):
        df_exist = _load_exist_data(fpath)
    if not isnull(df_exist):
        df_exist = df_exist[COLS_FINAL].copy()
        if df_exist.shape[0] > 0:
            start_month = df_exist['month'].max()
            start_month = str(start_month)[:6]
    
    if start_month >= end_month:
        logger_show('CPI最新数据已存在，不更新。', logger)
        _logger_show_last_month(df_exist)
        return df_exist
    
    logger_show('更新CPI数据, {}->{} ...'.format(start_month, end_month),
                logger, 'info')
    df = get_chn_cpi(start_month, end_month, ts_api)
    if df.shape[0] == 0:
        logger_show('新获取0条记录，返回已存在数据。',
                    logger, 'warn')
        _logger_show_last_month(df_exist)
        return df_exist
    
    # 统一字段名    
    df = df[COLS_FINAL].copy()
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


def update_chn_cpi_check(save_path=None,
                         root_dir=None,
                         start_month='195101',
                         ts_api=None,
                         logger=None):
    '''
    更新中国CPI数据
    '''
    
    def _get_save_path(save_path):
        '''获取中国CPI数据存放路径'''
        if isnull(save_path):
            from finfactory.load_his_data import find_target_dir
            save_dir = find_target_dir('chn_cpi/tushare/',
                       root_dir=root_dir, make=True, logger=logger)
            save_path = save_dir + 'chn_cpi.csv'
        return save_path
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    save_path = _get_save_path(save_path)    
    data_all = update_chn_cpi(df_exist=None,
                              fpath=save_path,
                              start_month=start_month,
                              end_month=None,
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
    def try_update_chn_cpi_check(*args, **kwargs):
        return update_chn_cpi_check(*args, **kwargs)
    
    
    df, loss = try_update_chn_cpi_check(
                                save_path=None,
                                root_dir=None,
                                start_month='195101',
                                ts_api=ts_api,
                                logger=logger)
        

    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')









