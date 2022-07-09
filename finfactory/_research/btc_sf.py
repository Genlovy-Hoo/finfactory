# -*- coding: utf-8 -*-

import time
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression as lr
from dramkit import load_csv, plot_series
from dramkit.datetimetools import date_add_nday
from dramkit.datsci import find_maxmin

#%%
if __name__ == '__main__':
    strt_tm = time.time()
    
    #%%
    fpath = '../../data/archives/btc/qkl123/BTC-S2F.csv'
    data = load_csv(fpath)
    data.columns = ['time', 'Price', 'S2F_m', 'S2F_y', 'expPrice_m',
                    'expPrice_y']
    if len(data['time'][0]) == 16:
        data['time'] = data['time'].apply(lambda x: x[:10])
    else:
        data['time'] = data['time'].apply(lambda x: x[4:15])
        data['time'] = pd.to_datetime(data['time'])
        data['time'] = data['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    data.set_index('time', inplace=True)
    
    # 价格和S2F取对数
    data['ln_Price'] = np.log(data['Price'])
    data['ln_S2F_m'] = np.log(data['S2F_m'])
    data['ln_expPrice_m'] = np.log(data['expPrice_m'])
    
    plot_series(data, {'Price': '-k', 'expPrice_m': '-b'}, grids=True)
    plot_series(data, {'ln_Price': '-k', 'ln_expPrice_m': '-b'}, grids=True)
    
    #%%    
    # 线性模型
    mdl = lr().fit(data[data.index >= '2013-01-01'][['ln_S2F_m']],
                    data[data.index >= '2013-01-01']['ln_Price'])
    # mdl = lr().fit(data[['ln_S2F_m']], data['ln_Price'])
    data['ln_Price_pred'] = mdl.predict(data[['ln_S2F_m']])
    data['ln_True_Pred'] = data['ln_Price'] / data['ln_Price_pred']
    
    plot_series(data[data.index >= '2013-01-01'],
                {'ln_Price': '-k', 'ln_S2F_m': '-b',
                       'ln_Price_pred': '-r'},
                cols_styl_up_right={'ln_True_Pred': '-y'},
                grids=False)
    
    data['Price_pred'] = np.exp(data['ln_Price_pred'])
    data['True_Pred'] = data['Price'] / data['Price_pred']
    data['True_Pred_qkl123'] = data['Price'] / data['expPrice_m']
    data['maxmin_True_Pred'] = find_maxmin(data['True_Pred'], t_min=300)
    data['maxmin_True_Pred_qkl123'] = find_maxmin(data['True_Pred_qkl123'],
                                                 t_min=300)
    plot_series(data, {'Price': ('-k', 'Price'),
                       'Price_pred': ('-r', 'Price_S/F-Pred')},
                cols_styl_up_right={'True_Pred': ('-b', 'Bubble')},
                cols_to_label_info={'True_Pred':
                        [['maxmin_True_Pred', (-1, 1), ('r^', 'gv'), False]]},
                legend_locs=[9], grids=False)
        
        
    data.sort_index(ascending=False, inplace=True)
    
    #%%
    # 未来预测
    last_date = data.index[0]
    his_half_dates = ['2012-11-28', '2016-07-10', '2020-05-12'] # 历史减半日期
    last_half_date = his_half_dates[-1]
    next_half_date = '2024-05-09' # 预计下次减半日期
    pre_final_date = '2026-01-01'
    # 下次减半时S2F预估
    tmps = []
    for k in range(len(his_half_dates)):
        if k != len(his_half_dates)-1:
            tmps.append(data[(his_half_dates[k] < data.index) & \
                             (data.index <= his_half_dates[k+1])]['S2F_m'])
        else:
            tmps.append(data[his_half_dates[k] < data.index]['S2F_m'])
    means = [x.mean() for x in tmps]
    mdl_ = lr().fit(pd.DataFrame(range(1, len(means)+1)), pd.Series(means))
    S2F_pred = mdl_.predict(pd.DataFrame([len(means)+1]))[0]
    df_f = pd.DataFrame(
                pd.date_range(date_add_nday(last_date, 1), pre_final_date),
                columns=['time'])
    df_f['time'] = df_f['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_f['S2F_m'] = np.nan
    df_f.loc[df_f[df_f['time'] <= next_half_date].index, 'S2F_m'] = means[-1]
    # df_f.loc[df_f[df_f['time'] > next_half_date].index, 'S2F_m'] = \
    #                     data[data.index > last_half_date]['S2F_m'].mean() * 2
    df_f.loc[df_f[df_f['time'] > next_half_date].index, 'S2F_m'] = S2F_pred 
    df_f.set_index('time', inplace=True)
    df_f['ln_S2F_m'] = np.log(df_f['S2F_m'])
    df_f['ln_Price_pred'] = mdl.predict(df_f[['ln_S2F_m']])
    df_f['Price_pred'] = np.exp(df_f['ln_Price_pred'])
    df_f = df_f.reindex(columns=data.columns)
    
    df_all = pd.concat((data, df_f), axis=0)
    df_all.sort_index(ascending=True, inplace=True)
    
    plot_series(df_all[df_all.index >= '2012-10-01'],
                {'ln_Price': '-k', 'ln_S2F_m': '-b',
                 'ln_Price_pred': '-r'},
                cols_styl_up_right={'ln_True_Pred': '-y'},
                # yparls_info_up=[(last_date, 'k', '-', 1.0)],
                yparls_info_up=[(x, 'k', '-', 1.0) for x in his_half_dates],
                grids=False)    
    plot_series(df_all,
                {'Price': ('-k', 'Price'),
                 'Price_pred': ('-r', 'Price_S/F-Pred')},
                cols_styl_up_right={'True_Pred': ('-b', 'Bubble')},
                cols_to_label_info={'True_Pred':
                        [['maxmin_True_Pred', (-1, 1), ('r^', 'gv'), False]]},
                # yparls_info_up=[(last_date, 'k', '-', 1.0)],
                yparls_info_up=[(x, 'k', '-', 1.0) for x in his_half_dates],
                legend_locs=[9], grids=False)
        
    print(f'\nPrice-Pred after next half:\n{df_f["Price_pred"].iloc[-1]}.')
    
    #%%
    bubble_maxs = data[data['maxmin_True_Pred'] == 1]['True_Pred']
    bubble_mins = data[data['maxmin_True_Pred'] == -1]['True_Pred']
    print('\nbubble_mins:')
    print(bubble_mins)
    print('\nbubble_maxs:')
    print(bubble_maxs)
    print('\nbubble_now:')
    print(data['True_Pred'].iloc[0])
    
    #%%
    bubble_maxs_qkl123 = data[data['maxmin_True_Pred_qkl123'] == 1]['True_Pred_qkl123']
    bubble_mins_qkl123 = data[data['maxmin_True_Pred_qkl123'] == -1]['True_Pred_qkl123']
    print('\nbubble_mins_qkl123:')
    print(bubble_mins_qkl123)
    print('\nbubble_maxs_qkl123:')
    print(bubble_maxs_qkl123)
    print('\nbubble_now_qkl123:')
    print(data['True_Pred_qkl123'].iloc[0])
        
    #%%
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    