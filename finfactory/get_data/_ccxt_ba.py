# -*- coding: utf-8 -*-

import os
from dramkit import simple_logger
from dramkit.iotools import cmd_run_pys


if __name__ == '__main__':    
    cwd = os.getcwd()
    

    files = [
        'ccxt_1d_ba.py',
        'ccxt_15min_ba.py',
        'ccxt_5min_ba.py',
        # 'ccxt_1min_ba.py'
    ]
    # files = [os.path.join(cwd, f) for f in files]
    
    # logger = simple_logger(os.path.join(cwd, '../../log/get_data_ccxt_ba.log'), 'a')
    logger = None
    
    cmd_run_pys(files, logger)
    