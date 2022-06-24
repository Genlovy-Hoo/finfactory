# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
from dramkit import logger_show, isnull, load_csv
from dramkit.iotools import get_last_change_time
from dramkit.datetimetools import date_reformat, get_year_month
from dramkit.datetimetools import get_recent_workday_chncal
from dramkit.other.othertools import archive_data
from finfactory.load_his_data import find_target_dir
from finfactory.utils.utils import get_tushare_api


def get_options_info(exchange, ts_api=None, logger=None):
    '''
    | tushare获取给定交易所exchange的期权合约信息
    | exchange表示交易所（与tushare官方文档一致）:
    |     CFFEX-中金所 DCE-大商所 CZCE-郑商所 SHFE-上期所
    |     SSE-上交所 SZSE-深交所
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    cols = {'ts_code': 'code',
            'exchange': '交易市场',
            'name': '名称',
            'per_unit':'合约单位',
            'opt_code': '标的证券代码', # '标准合约代码',
            'opt_type': '合约类型',
            'call_put': '期权类型',
            'exercise_type': '行权方式', 
            'exercise_price': '行权价格',
            's_month': '结算月', 
            'maturity_date': '到期日',
            'list_price': '挂牌基准价',
            'list_date': '开始交易日期',
            'delist_date': '最后交易日期',
            'last_edate': '最后行权日期',
            'last_ddate': '最后交割日期',
            'quote_unit': '报价单位',
            'min_price_chg': '最小价格波幅'
        }
    if exchange in ['DCE']:
        # 看涨期权
        df_C = ts_api.opt_basic(exchange=exchange, call_put='P',
                                fields=','.join(list(cols.keys())))
        logger_show('pausing...', logger)
        time.sleep(65)
        # 看跌期权
        df_P = ts_api.opt_basic(exchange=exchange, call_put='C',
                                fields=','.join(list(cols.keys())))
        df = pd.concat((df_C, df_P), axis=0)
    else:
        df = ts_api.opt_basic(exchange=exchange,
                              fields=','.join(list(cols.keys())))
    df.rename(columns=cols, inplace=True)
    opt_type_map = {'C': '看涨期权','P': '看跌期权'}
    df['期权类型'] = df['期权类型'].map(opt_type_map)    
    for col in ['到期日', '开始交易日期', '最后交易日期', '最后行权日期',
                '最后交割日期']:
        df[col] = df[col].apply(lambda x:
                  date_reformat(x, '-') if not isnull(x) else np.nan)
    for col in ['结算月']:
        df[col] = df[col].apply(lambda x:
                  str(x)[:4]+'-'+str(x)[4:6] if not isnull(x) else np.nan)
    df['到期月'] = df['到期日'].apply(lambda x: get_year_month(x))
    return df


def update_options_info(exchange, save_path=None, root_dir=None,
                        ts_api=None, logger=None):
    '''更新给定交易所的期权合约基本信息'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if save_path is None:
        save_dir = find_target_dir('options/tushare/options_info/',
                   root_dir=root_dir, make=True, logger=logger)
        save_path = save_dir + '{}.csv'.format(exchange)
    assert not isnull(save_path), '`save_path`不能为无效值！'
    
    end_date = get_recent_workday_chncal(dirt='pre')
    if os.path.exists(save_path) and \
       (get_last_change_time(save_path, '%Y-%m-%d') >= end_date):
        logger_show('{}期权合约信息数据已是最新，不更新！'.format(exchange),
                    logger, 'info')
        return load_csv(save_path, encoding='gbk'), False
    
    logger_show('更新{}期权合约基本信息数据...'.format(exchange),
                logger, 'info')
    df = get_options_info(exchange, ts_api, logger)
    if df.shape[0] == 0 or isnull(df):
        logger_show('{}新获取0条记录！'.format(exchange),
                    logger, 'warn')
        if os.path.exists(save_path):
            return load_csv(save_path, encoding='gbk'), True
        else:
            return None, True
    
    if os.path.exists(save_path):
        df_exist = load_csv(save_path, encoding='gbk')
    else:
        df_exist = None
    # 数据合并
    df = archive_data(df, df_exist,
                      sort_cols=['到期日', 'code'],
                      del_dup_cols=['code'],
                      sort_first=True,
                      csv_path=save_path,
                      csv_index=None,
                      csv_encoding='gbk')
    
    return df, True


if __name__ == '__main__':
    import sys
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    ts_api = get_tushare_api()
    logger = gen_py_logger(sys.argv[0])
    
    
    @try_repeat_run(cfg.try_get_tushare, logger=logger,
                    sleep_seconds=cfg.try_get_tushare_sleep)
    def try_update_options_info(*args, **kwargs):
        return update_options_info(*args, **kwargs)
    
    
    exs = {
            'SSE': ['上交所(上海证券交易所)', '.SH'],
            'SZSE': ['深交所(深圳证券交易所)', '.SZ'],
            'CFFEX': ['中金所(中国金融期权交易所)', '.CFX'],
            'CZCE': ['郑商所(郑州商品交易所)', '.ZCE'],
            'SHFE': ['上期所(上海期权交易所)', '.SHF'],
            'DCE': ['大商所(大连商品交易所)', '.DCE']
        }
    
    exs_ = list(exs.keys())
    for k in range(len(exs_)):
        ex = exs_[k]
        exec('''df_{}, updated = try_update_options_info(
                                    ex, 
                                    save_path=None,
                                    root_dir=None,
                                    ts_api=ts_api,
                                    logger=logger)
              '''.format(ex)
              )
        if eval('updated') and k != len(exs_)-1:
            logger_show('pausing...', logger)
            time.sleep(65)
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
