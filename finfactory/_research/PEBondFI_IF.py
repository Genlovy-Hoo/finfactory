# -*- coding: utf-8 -*-

import pandas as pd
from dramkit.datsci.stats import cum_pct_loc
import finfactory.load_his_data as lhd
from finfactory.fintools.ahr999 import get_ahr999

#%%
def fut2index(code):
    code = code.lower()
    if 'if' in code:
        return '000300.SH'
    elif 'ic' in code:
        return '000905.SH'
    elif 'ih' in code:
        return '000016.SH'
    else:
        raise ValueError('未识别`{}`对应的指数代码！'.format(code))


def load_index_pe(code, dtype):
    if dtype == 'future':
        code = fut2index(code)
    df_pe = lhd.load_index_daily_basic_tushare(code)
    if 'pe_ttm' in df_pe.columns:
        df_pe['pe'] = df_pe['pe_ttm']
    df_pe = df_pe.set_index('date')[['pe', 'pb']]
    return df_pe


def load_data(code):
    '''导入历史数据，code为`IF`或`IC`或`IH`'''
    
    if code[:2].lower() in ['if', 'ic', 'ih']:
        dtype = 'future'
    else:
        dtype = 'index'
    
    if dtype == 'future':
        data = lhd.load_future_daily_tushare(code)#.iloc[:-1, :]
    elif dtype == 'index':
        data = lhd.load_index_daily_tushare(code)#.iloc[:-1, :]
    try:
        data.set_index('date', inplace=True)
    except:
        data.rename(columns={'time': 'date'}, inplace=True)
        data.set_index('date', inplace=True)
        
    df_pe = load_index_pe(code, dtype)    
    data = pd.merge(data, df_pe, how='left', left_index=True,
                    right_index=True)
    data.sort_index(ascending=True, inplace=True)
    # data['pe'] = data['pe'].interpolate('linear')
    
    df_bond = lhd.load_chn_bond_yields()
    df_bond.set_index('date', inplace=True)
    data = pd.merge(data, df_bond, how='left', left_index=True,
                    right_index=True)
    data.sort_index(ascending=True, inplace=True)
    data.index.name = 'date'
    data.reset_index(inplace=True)
    
    cols = ['date', 'close', 'open', 'volume', 'pe', 'pb',
            '3月', '6月', '1年', '2年', '3年', '5年',
            '7年', '10年', '30年']
    data = data[cols]
    
    return data

#%%
def get_pebondpb(data, period='10年'):
    '''
    计算估值和百分位
    data需包含['date', 'pe', 'pb']及指定期限国债收益率对应列`period`
    '''
    
    df = data.copy()    
    df['溢价率'] = 100 / df['pe'] - df[f'{period}']    
    df['估值百分位'] = 1 - cum_pct_loc(df['溢价率'], method='dense')    
    df['pe百分位'] = cum_pct_loc(df['pe'], method='dense')    
    df['pb百分位'] = cum_pct_loc(df['pb'], method='dense')
    
    return df


def get_fi(data, plot=False):
    '''
    计算FI
    data中需包含['date', 'close']两列
    '''
    
    df = data.copy()
    df['FI'] = get_ahr999(df, '2010-01-01', n_fix_inv=200,
                          fit_log=False, poly=2, model='lr', plot=plot)
    df['FI百分位'] = cum_pct_loc(df['FI'], method='dense')

    return df

#%%
if __name__ == '__main__':
    import time
    from dramkit import plot_series
    from finfactory.fintools import cross2_plot
    from finfactory.fintools.utils_gains import get_yield_curve
    from finfactory.fintools.utils_gains import signal_merge
    
    strt_tm = time.time()
    
    #%%
    code = '000300.SH'
    period = '10年'
    
    data = load_data(code)    
    data = data[['date', 'close', 'open', 'pe', 'pb', f'{period}']]
    data.set_index('date', drop=False, inplace=True)
    
    # data.fillna(method='ffill', inplace=True)
    # data['pe'] = data['pe'].interpolate('linear')
    # data['pb'] = data['pb'].interpolate('linear')
    # data['10年'] = data['10年'].interpolate('linear')
    
    data.dropna(how='any', inplace=True)
    
    #%% 
    # 溢价率
    df = get_pebondpb(data, period=period)
    
    # df = df[df['date'] >= '2015-01-01'].copy()
    
    plot_series(df, {'close': 'k-'},
                cols_styl_up_right={'估值百分位': ('-b', 'Vpct')},
                xparls_info={'估值百分位': [(0.43,), (0.18,), (0.68,)]},
                title=code)
    
    # # 10年: (6.3, 2.5); 5年: (6.5, 2.7); 3年: (6.5, 3.0)
    # # 1年: (7, 3.2); 6月: (7， 3.5); 3月: (7， 3.5)
    # df['sig1'] = cross2_plot(df, '溢价率', 6.3, '溢价率', 2.5,
    #                         buy_sig=-1, sel_sig=1)
    
    # trade_gain_info1, df_gain1 = get_yield_curve(df, 'sig1', sig_type=1,
    #         nn=252, net_type='fundnet', ext_type=1, shift_lag=1,
    #         col_price='close', col_price_buy='open', col_price_sel='open',
    #         settle_after_act=True, func_vol_add='all', func_vol_sub='hold_all',
    #         func_vol_stoploss='hold_1', func_vol_stopgain='hold_1',
    #         stop_no_same=True, ignore_no_stop=False,
    #         hold_buy_max=None, hold_sel_max=None,
    #         limit_min_vol=1, base_money=None, base_vol=1, init_cash=10000,
    #         fee=0.4/1000, max_loss=None, max_gain=None, max_down=None,
    #         stop_sig_order='both', force_final0=False, sos_money=1000,
    #         del_begin0=True, gap_repeat=None, show_sigs=True,
    #         show_dy_maxdown=False, show_key_infos=True, nshow=500, logger=None,
    #         plot=True, kwargs_plot={'figsize': (11, 6.5),
    #                                 'n_xticks': 6,
    #                                 'title': code})
    
    # plot_series(df_gain1,
    #             {'净值': '.-k'},
    #             cols_to_label_info={
    #                 '净值': [['inMaxDown_net', (1,), ('.-r',), ('MaxDown',)]]},
    #             title=code)
    
    #%%
    # FI
    df = get_fi(df, plot=False)
    
    df['sig2'] = cross2_plot(df, 'FI', 0.65, 'FI', 1.38,
                            buy_sig=-1, sel_sig=1)
    
    # trade_gain_info2, df_gain2 = get_yield_curve(df, 'sig2', sig_type=1,
    #         nn=252, net_type='fundnet', ext_type=1, shift_lag=1,
    #         col_price='close', col_price_buy='open', col_price_sel='open',
    #         settle_after_act=True, func_vol_add='all', func_vol_sub='hold_all',
    #         func_vol_stoploss='hold_1', func_vol_stopgain='hold_1',
    #         stop_no_same=True, ignore_no_stop=False,
    #         hold_buy_max=None, hold_sel_max=None,
    #         limit_min_vol=1, base_money=None, base_vol=1, init_cash=10000,
    #         fee=0.4/1000, max_loss=None, max_gain=None, max_down=None,
    #         stop_sig_order='both', force_final0=False, sos_money=1000,
    #         del_begin0=True, gap_repeat=None, show_sigs=True,
    #         show_dy_maxdown=False, show_key_infos=True, nshow=500, logger=None,
    #         plot=True, kwargs_plot={'figsize': (11, 6.5),
    #                                 'n_xticks': 6,
    #                                 'title': code})
    
    # plot_series(df_gain2,
    #             {'净值': '.-k'},
    #             cols_to_label_info={
    #                 '净值': [['inMaxDown_net', (1,), ('.-r',), ('MaxDown',)]]},
    #             title=code)
    
    #%%
    df['sig'] = signal_merge(df, 'sig1', 'sig2', merge_type=3)
    
    # trade_gain_info3, df_gain3 = get_yield_curve(df, 'sig', sig_type=1,
    #         nn=252, net_type='fundnet', ext_type=1, shift_lag=1,
    #         col_price='close', col_price_buy='open', col_price_sel='open',
    #         settle_after_act=True, func_vol_add='all', func_vol_sub='hold_all',
    #         func_vol_stoploss='hold_1', func_vol_stopgain='hold_1',
    #         stop_no_same=True, ignore_no_stop=False,
    #         hold_buy_max=None, hold_sel_max=None,
    #         limit_min_vol=1, base_money=None, base_vol=1, init_cash=10000,
    #         fee=0.4/1000, max_loss=None, max_gain=None, max_down=None,
    #         stop_sig_order='both', force_final0=False, sos_money=1000,
    #         del_begin0=True, gap_repeat=None, show_sigs=True,
    #         show_dy_maxdown=False, show_key_infos=True, nshow=500, logger=None,
    #         plot=True, kwargs_plot={'figsize': (11, 6.5),
    #                                 'n_xticks': 6,
    #                                 'title': code})
    
    # plot_series(df_gain3,
    #             {'净值': '.-k'},
    #             cols_to_label_info={
    #                 '净值': [['inMaxDown_net', (1,), ('.-r',), ('MaxDown',)]]},
    #             title=code)
        
    #%%
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    
    
    
    
    
    