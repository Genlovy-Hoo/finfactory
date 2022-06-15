# -*- coding: utf-8 -*-

import os
from dramkit import simple_logger
from dramkit.iotools import cmd_run_pys


if __name__ == '__main__':    
    cwd = os.getcwd()
    

    files = [
        'tushare_trade_dates.py',
        'tushare_index_daily.py',
        
        'cffex_futures_lhb.py',
        
        'chn_national_bond_rates.py',
        'chn_local_bond_rates.py'
    ]
    # files = [os.path.join(cwd, f) for f in files]
    
    # logger = simple_logger(os.path.join(cwd, '../../log/get_data_tushare.log'), 'a')
    logger = None
    
    cmd_run_pys(files, logger)
        
        
        
    