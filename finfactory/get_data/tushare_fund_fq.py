# -*- coding: utf-8 -*-


from dramkit import load_csv, isnull


def fund_qfq(data_path, dividend_path, data_qfq_path=None):
    '''基金前复权，根据分红记录'''
    data = load_csv(data_path)
    
    dividend = load_csv(dividend_path, encoding='gbk').dropna(how='any')
    dividend = dividend.reindex(columns=['权益登记日', '每份分红'])
    dividend.rename(columns={'权益登记日': 'date', '每份分红': 'r'}, inplace=True)
    dividend['r'] = dividend['r'].apply(lambda x: float(x[5:-1]))
    dividend.sort_values('date', ascending=False, inplace=True)
    dividend['r_pre'] = dividend['r'].cumsum()
    
    data = data.merge(dividend, how='left', on=['date'])
    data.sort_values('date', ascending=False, inplace=True)
    data.loc[data.index[0], 'r_pre'] = 0 if isnull(data['r_pre'].iloc[0]) else data['r_pre'].iloc[0]
    data['r_pre'] = data['r_pre'].fillna(method='ffill')
    
    data['pre_close'] = data['pre_close'] - data['r_pre'] 
    data['open'] = data['open'] - data['r_pre']
    data['low'] = data['low'] - data['r_pre']
    data['high'] = data['high'] - data['r_pre']
    data['close'] = data['close'] - data['r_pre']
    data['change_pct'] = (data['close'] / data['pre_close'] - 1) * 100
    
    data.sort_values('date', ascending=True, inplace=True)
    if not isnull(data_qfq_path):
        data.to_csv(data_qfq_path, index=None)
    
    return data


if __name__ == '__main__':
    import shutil
    from finfactory.load_his_data import find_target_dir
    
    
    codes = [
        '510050.SH', # 上证50ETF
        '510300.SH', # 沪深300ETF（沪）
        '159919.SZ', # 沪深300ETF（深）
        '510500.SH' # 中证500ETF
    ]
    
    
    dfs = {}
    for code in codes:
        fdir = find_target_dir('fund/tushare/{}/'.format(code)) 
        fpath_daily = '{}{}_daily.csv'.format(fdir, code)
        fpath_qfq = '{}{}_daily_qfq.csv'.format(fdir, code)
        fpath_div = '{}{}分红记录.csv'.format(fdir, code[:6])
        df_qfq = fund_qfq(fpath_daily, fpath_div, fpath_qfq)
        dfs[code] = df_qfq
        
        if '510050' in code:
            shutil.copy(fpath_qfq,
                        '../../../DramKit/dramkit/_test')
            shutil.copy(fpath_qfq,
                        '../_test')
            
            
            
            
            
