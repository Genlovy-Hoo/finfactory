# -*- coding: utf-8 -*-

import os
import sys
from dramkit import simple_logger
from dramkit.iotools import cmd_run_pys
from finfactory.utils.utils import gen_py_logger
from finfactory.config import cfg as config

config.set_key_value('no_py_log', False)


if __name__ == '__main__':    
    cwd = os.getcwd()
    

    files = [
        'tushare_trade_dates.py',
        
        'tushare_index_info.py',
        'tushare_index_daily.py',
        'tushare_index_daily_basic.py',
        'eniu_index_pe.py',
        
        'tushare_chn_money.py',
        
        'tushare_futures_info.py',
        'tushare_futures_mapping.py',
        'tushare_futures_daily.py',
        
        'tushare_options_info.py',
        
        'chn_national_bond_rates.py',
        'chn_local_bond_rates.py',
        
        'cffex_futures_lhb.py',
        
        'tushare_futures_daily_ex.py',
        'tushare_options_daily.py',
        
        'eastmoney_tonorth_netbuy_daily.py',
        'eastmoney_tonorth_netin_daily.py',
        'eastmoney_tosouth_netbuy_daily.py',
        'eastmoney_tosouth_netin_daily.py',
        
        'hexun_gold_daily.py',
        'hexun_silver_daily.py',
        
        'sw_daily_info.py',
        
        # 'fundex_index_dpe.py'
    ]
    # files = [os.path.join(cwd, f) for f in files]
    
    # logger = simple_logger(
    #            os.path.join(cwd, '../../log/get_data.log'),
    #            'a')
    logger = gen_py_logger(sys.argv[0])
    # logger = None
    
    cmd_run_pys(files, logger)
        
        
        
    