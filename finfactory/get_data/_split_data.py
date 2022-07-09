# -*- coding: utf-8 -*-    

if __name__ == '__main__':
    import time
    from dramkit.iotools import cut_csv_by_year
    from finfactory.load_his_data import find_target_dir
    
    strt_tm = time.time()
    
    
    # # 股票日线行情
    # fdir = find_target_dir('stocks/tushare/')
    # fpath = f'{fdir}stocks_daily/astocks_daily.csv'
    # print('cut astocks daily...')
    # cut_csv_by_year(fpath, tcol='date')
    
    # # 股票日线行情日指标
    # fdir = find_target_dir('stocks/tushare/')
    # fpath = f'{fdir}stocks_daily/astocks_daily_basic.csv'
    # print('cut astocks daily_basic...')
    # cut_csv_by_year(fpath, tcol='date')
    
    # # 股票日线行情_stk
    # fdir = find_target_dir('stocks/tushare/')
    # fpath = f'{fdir}stocks_daily/astocks_daily_stk.csv'
    # print('cut astocks daily_stk...')
    # cut_csv_by_year(fpath, tcol='date')
    
    
    # # 交易所期货日线行情
    # exs = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE']
    # for ex in exs:
    #     print(f'cut {ex} futures daily...')
    #     fdir = find_target_dir('futures/tushare/futures_daily/')
    #     fpath = fdir + '{}.csv'.format(ex)
    #     cut_csv_by_year(fpath, tcol='date', name_last_year=False,
    #                     kwargs_loadcsv={'encoding': 'gbk'},
    #                     kwargs_tocsv={'index': None,
    #                                   'encoding': 'gbk'})
    
    
    # # 交易所期权日线行情
    # exs = ['SSE', 'SZSE', 'CFFEX', 'CZCE', 'SHFE', 'DCE']
    # for ex in exs:
    #     print(f'cut {ex} options daily...')
    #     fdir = find_target_dir('options/tushare/options_daily/')
    #     fpath = fdir + '{}.csv'.format(ex)
    #     cut_csv_by_year(fpath, tcol='date')
        
        
    # # 申万指数日行情
    # fpath = find_target_dir('sw/') + 'sw_daily.csv'
    # print('cut sw daily...')
    # cut_csv_by_year(fpath, tcol='date', name_last_year=False,
    #                 kwargs_loadcsv={'encoding': 'gbk'},
    #                 kwargs_tocsv={'index': None,
    #                               'encoding': 'gbk'})
    
    
    # # 数字货币一分钟线
    # minute = 1
    # mkt = 'binance'
    # name12 = [('btc', 'btc_usdt'), ('eth', 'eth_usdt')]
    # for name1, name2 in name12:
    #     print(f'cut {name2} {minute}min ...')
    #     fdir = find_target_dir('{}/ccxt_{}/'.format(name1, mkt))
    #     fpath = fdir + '{}_{}minute.csv'.format(name2, int(minute))
    #     cut_csv_by_year(fpath, tcol='time')
        
    
    print('used: {}s.'.format(round(time.time()-strt_tm, 6)))
    
    
    
    
    
    
