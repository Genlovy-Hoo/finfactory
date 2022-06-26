# -*- coding: utf-8 -*-

import os
import numpy as np
from dramkit import logger_show, isnull, load_csv
from dramkit.iotools import get_last_change_time
from dramkit.datetimetools import date_reformat
from dramkit.datetimetools import get_recent_workday_chncal
from dramkit.other.othertools import archive_data
from finfactory.load_his_data import find_target_dir
from finfactory.utils.utils import get_tushare_api


def get_index_info(market, ts_api=None):
    '''
    | tushare获取给定市场market的指数信息
    | market表示交易市场（与tushare官方文档一致）:
    |     MSCI MSCI指数, CSI 中证指数, SSE 上交所指数, SZSE 深交所指数
    |     CICC 中金指数, SW 申万指数, OTH 其他指数
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    cols = {'ts_code': 'code',
            'name': '简称',
            'fullname': '指数全称',
            'market': '市场',
            'publisher': '发布方',
            'index_type': '指数风格',
            'category': '指数类别',
            'base_date': '基期',
            'base_point': '基点',
            'list_date': '发布日期',
            'weight_rule': '加权方式',
            'desc': '描述',
            'exp_date': '终止日期'
        }
    df = ts_api.index_basic(market=market,
                            fields=','.join(list(cols.keys())))
    df.rename(columns=cols, inplace=True)
    for col in ['基期', '发布日期', '终止日期']:
        df[col] = df[col].apply(lambda x:
                  date_reformat(x, '-') if not isnull(x) else np.nan)
    return df


def update_index_info(market, save_path=None, root_dir=None,
                      ts_api=None, logger=None):
    '''更新给定市场的指数基本信息'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if save_path is None:
        save_dir = find_target_dir('index/tushare/index_info/',
                   root_dir=root_dir, make=True, logger=logger)
        save_path = save_dir + '{}.csv'.format(market)
    assert not isnull(save_path), '`save_path`不能为无效值！'
    
    end_date = get_recent_workday_chncal(dirt='pre')    
    if os.path.exists(save_path) and \
       (get_last_change_time(save_path, '%Y-%m-%d') >= end_date):
        logger_show('{}指数基本数据已是最新，不更新！'.format(market),
                    logger, 'info')
        return load_csv(save_path, encoding='gbk')
    
    logger_show('更新{}指数基本信息数据...'.format(market),
                logger, 'info')
    df = get_index_info(market, ts_api)
    if df.shape[0] == 0 or isnull(df):
        logger_show('{}新获取0条记录！'.format(market),
                    logger, 'warn')
        if os.path.exists(save_path):
            return load_csv(save_path, encoding='gbk')
        else:
            return None
    
    if os.path.exists(save_path):
        df_exist = load_csv(save_path, encoding='gbk')
    else:
        df_exist = None
    # 数据合并
    df = archive_data(df, df_exist,
                      sort_cols=['发布日期', 'code'],
                      del_dup_cols=['code'],
                      sort_first=True,
                      csv_path=save_path,
                      csv_index=None,
                      csv_encoding='gbk')
    
    return df


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
    def try_update_index_info(*args, **kwargs):
        return update_index_info(*args, **kwargs)
    
    
    mkts = {
        'MSCI':	'MSCI指数',
        'CSI': '中证指数',
        'SSE': '上交所指数',
        'SZSE': '深交所指数',
        'CICC': '中金指数',
        'SW': '申万指数',
        'OTH': '其他指数',
    }
    for mkt in mkts.keys():
        exec('''df_{} = try_update_index_info(
                            mkt, 
                            save_path=None,
                            root_dir=None,
                            ts_api=ts_api,
                            logger=logger)
             '''.format(mkt)
             )
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
