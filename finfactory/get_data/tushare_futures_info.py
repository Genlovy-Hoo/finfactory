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


def get_futures_info(exchange, ts_api=None):
    '''
    | tushare获取给定交易所exchange的期货合约信息
    | exchange表示交易所（与tushare官方文档一致）:
    |     CFFEX-中金所 DCE-大商所 CZCE-郑商所 SHFE-上期所
    |     INE-上海国际能源交易中心
    '''
    if isnull(ts_api):
        ts_api = get_tushare_api()
    cols = {'ts_code': 'code',
            'symbol': '交易标识',
            'exchange': '交易市场',
            'name': '简称',
            'fut_code': '合约产品代码',
            'multiplier': '合约乘数',
            'trade_unit': '交易计量单位',
            'per_unit': '交易单位(每手)',
            'quote_unit': '报价单位',
            'quote_unit_desc': '最小报价单位说明',
            'd_mode_desc': '交割方式说明',
            'list_date': '上市日期',
            'delist_date': '最后交易日期',
            'd_month': '交割月份',
            'last_ddate': '最后交割日',
            'trade_time_desc': '交易时间说明'
        }
    df = ts_api.fut_basic(exchange=exchange,
                          fields=','.join(list(cols.keys())))
    df.rename(columns=cols, inplace=True)
    for col in ['上市日期', '最后交易日期', '最后交割日']:
        df[col] = df[col].apply(lambda x:
                  date_reformat(x, '-') if not isnull(x) else np.nan)
    df['交割月份'] = df['交割月份'].apply(lambda x:
                    str(x)[:4]+'-'+str(x)[4:6] if not isnull(x) else np.nan)
    return df


def update_futures_info(exchange, save_path=None, root_dir=None,
                        ts_api=None, logger=None):
    '''更新给定交易所的期货合约基本信息'''
    
    if isnull(ts_api):
        ts_api = get_tushare_api()
    
    if save_path is None:
        save_dir = find_target_dir('futures/tushare/futures_info/',
                   root_dir=root_dir, make=True, logger=logger)
        save_path = save_dir + '{}.csv'.format(exchange)
    assert not isnull(save_path), '`save_path`不能为无效值！'
    
    end_date = get_recent_workday_chncal(dirt='pre')
    if os.path.exists(save_path) and \
       (get_last_change_time(save_path, '%Y-%m-%d') >= end_date):
        logger_show('{}期货合约信息数据已是最新，不更新！'.format(exchange),
                    logger, 'info')
        return load_csv(save_path, encoding='gbk')
    
    logger_show('更新{}期货合约基本信息数据...'.format(exchange),
                logger, 'info')
    df = get_futures_info(exchange, ts_api)
    if df.shape[0] == 0 or isnull(df):
        logger_show('{}新获取0条记录！'.format(exchange),
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
                      sort_cols=['上市日期', 'code'],
                      del_dup_cols=['code'],
                      sort_first=False,
                      csv_path=save_path,
                      csv_index=None,
                      csv_encoding='gbk')
    df.reset_index(drop=True, inplace=True)
    
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
    def try_update_futures_info(*args, **kwargs):
        return update_futures_info(*args, **kwargs)
    
    
    exs = {
            'CFFEX': '中金所',
            'DCE': '大商所',
            'CZCE': '郑商所',
            'SHFE': '上期所',
            'INE': '上海国际能源交易中心',
        }
    
    dfs = {}
    for ex in exs.keys():
        exec('''dfs['{}'] = try_update_futures_info(
                            ex, 
                            save_path=None,
                            root_dir=None,
                            ts_api=ts_api,
                            logger=logger)
              '''.format(ex)
              )
        
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
