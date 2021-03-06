# -*- coding: utf-8 -*-

import pandas as pd
from dramkit import plot_series
from dramkit import simple_logger
from finfactory.fintools import get_yield_curve


if __name__ == '__main__':
    # 导入数据
    data = pd.read_excel('./test_signal.xlsx')
    data['date'] = data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    data.set_index('date', inplace=True)
    
    # 交易量
    data['vol'] = data['sig_combine2'].diff().fillna(0) 
    # 曲线生成
    trade_gain_info, df_gain = get_yield_curve(
                                    data, 'vol', sig_type=2,
                                    del_begin0=True,
                                    logger=simple_logger('./fin_plot_test.log'))
    # 重新画图
    # 先要区分平仓信号
    df = df_gain.reindex(columns=['close', 'vol', 'act', 'holdVol'])
    df['act'] = df[['act', 'holdVol']].apply(lambda x:
                  2 if x['holdVol'] == 0 and x['act'] != 0 else x['act'], axis=1)
    plot_series(df, {'close': ('-k', 'close')},
                cols_to_label_info={
                    'close': [['act', (1, -1, 2), ('gv', 'r^', 'bo'),
                                ('开空', '开多', '平仓')]]})
    
    
    