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
        'ccxt_1d.py',
        'ccxt_minute.py'
    ]
    # files = [os.path.join(cwd, f) for f in files]
    
    # logger = simple_logger(
    #            os.path.join(cwd, '../../log/get_data_ccxt.log'),
    #            'a')
    logger = gen_py_logger(sys.argv[0])
    # logger = None
    
    cmd_run_pys(files, logger)
    