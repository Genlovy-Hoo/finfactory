# -*- coding: utf-8 -*-

import os
import sys
import time
from dramkit import logger_show
from dramkit.iotools import cmd_run_pys
from finfactory.utils.utils import gen_py_logger
from finfactory.config import cfg as config

config.set_key_value('no_py_log', False)


if __name__ == '__main__':    
    cwd = os.getcwd()    
    strt_tm = time.time()

    files = [
        'ccxt_1d.py',
        'ccxt_minute.py'
    ]
    
    logger = gen_py_logger(sys.argv[0])
    # logger = None
    
    cmd_run_pys(files, logger)
    
    
    us = round(time.time()-strt_tm, 6)
    logger_show('cmd run pys used time: {}s.'.format(us), logger)
    