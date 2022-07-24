# -*- coding: utf-8 -*-

import traceback
from dramkit import simple_logger
from dramkit import logger_show
try:
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.iotools import cmd_run_pys
    from finfactory.utils.utils import gen_py_logger
    from finfactory.config import cfg as config
    
    config.set_key_value('no_py_log', False)


    strt_tm = time.time()

    files = [
        'ccxt_1d.py',
        'ccxt_minute.py'
    ]
    
    logger = gen_py_logger(sys.argv[0], config=config)
    # logger = None
    
    logger_show('\n{}'.format('-'*120), logger)
    time.sleep(0.2)
    cmd_run_pys(files, logger)
    
    
    us = round(time.time()-strt_tm, 6)
    logger_show('cmd run pys used time: {}s.'.format(us), logger)
    close_log_file(logger)
except:
    logger = simple_logger('../../log/_ccxt.log', 'a')
    logger_show(traceback.format_exc(), logger, 'error')
    logger_show('运行出错！', logger, 'error')
    close_log_file(logger)
    