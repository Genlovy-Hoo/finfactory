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
        'tushare_trade_dates.py',
        'tushare_astocks_list.py',
        
        'tushare_index_info.py',
        'tushare_index_daily.py',
        'tushare_index_daily_basic.py',
        'eniu_index_pe.py',
        'tushare_rzrq_daily.py',
        
        'tushare_chn_money.py',
        'tushare_chn_gdp.py',
        'tushare_chn_cpi.py',
        'tushare_chn_ppi.py',
        'tushare_usa_bond_rates.py',
        'tushare_usa_bond_rates_act.py',
        'tushare_shibor.py',
        'tushare_lpr.py',
        
        'tushare_fund_list.py',
        'tushare_fund_daily.py',
        'tushare_fund_adj.py',
        'tushare_fund_fq.py',
        
        'tushare_block_trades.py',
        
        'tushare_futures_info.py',
        'tushare_futures_mapping.py',
        'tushare_futures_daily.py',
        'tushare_futures_daily_ex.py',
        
        'tushare_options_info.py',
        'tushare_options_daily.py',
        
        'tushare_cctv_news.py',
        
        'chn_national_bond_rates.py',
        'chn_local_bond_rates.py',
        
        'cffex_futures_lhb.py',
        
        'eastmoney_tonorth_netbuy_daily.py',
        'eastmoney_tonorth_netin_daily.py',
        'eastmoney_tosouth_netbuy_daily.py',
        'eastmoney_tosouth_netin_daily.py',
        
        'sw_daily_info.py',
        
        'fundex_index_dpe.py',
        
        'tushare_stocks_daily.py',
        'tushare_stocks_daily_basic.py',
        'tushare_stocks_daily_stk.py',
        # 'tushare_stock_daily.py',
        # 'tushare_stock_daily_basic.py',
        # 'tushare_stock_daily_stk.py',
        
        # 'hexun_gold_daily.py',
        # 'hexun_silver_daily.py',
    ]
    
    logger = gen_py_logger(sys.argv[0], config=config)
    # logger = None
    
    cmd_run_pys(files, logger)
    
    
    us = round(time.time()-strt_tm, 6)
    logger_show('cmd run pys used time: {}s.'.format(us), logger)
    close_log_file(logger)

except:
    logger = simple_logger('../../log/_get_data.log', 'a')
    logger_show(traceback.format_exc(), logger, 'error')
    logger_show('运行出错！', logger, 'error')
    close_log_file(logger)
    
    
        
        
    