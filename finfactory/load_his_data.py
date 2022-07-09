# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
from pathlib import Path
from dramkit import isnull
from dramkit import load_csv
from dramkit import logger_show
from dramkit.iotools import make_dir
from dramkit.iotools import load_csvs
from dramkit.gentools import merge_df
from dramkit.datetimetools import date_reformat
from dramkit.other.othertools import load_text_multi
from finfactory.fintools.utils_chn import get_code_ext
from finfactory.utils.utils import check_date_loss

#%%
FILE_PATH = Path(os.path.realpath(__file__))

TS_NAME_CODE = {
        '上证指数': '000001.SH', 
        '创业板指': '399006.SZ',
        '中小板指': '399005.SZ', 
        '上证50': '000016.SH',
        '沪深300': '000300.SH',
        '中证500': '000905.SH',
        '中证1000': '000852.SH',
        '科创50': '000688.SH',
        '深证成指': '399001.SZ',
    }
for x in ['IF', 'IC', 'IH']:
    TS_NAME_CODE.update({x: x+'.CFX'})
    TS_NAME_CODE.update({x+'9999': x+'.CFX'})
    TS_NAME_CODE.update({x.lower(): x+'.CFX'})
    TS_NAME_CODE.update({x.lower()+'9999': x+'.CFX'})

#%%
def find_target_dir(dir_name, root_dir=None, make=False,
                    logger=None):
    assert isinstance(root_dir, (type(None), str))
    if root_dir is None:
        from finfactory.config import cfg
        prefix_dirs = cfg.archive_roots.copy()
    else:
        prefix_dirs = [root_dir]
    fpath = str(FILE_PATH.parent.parent)
    fpath = os.path.join(fpath, 'data', 'archives/')
    fpath = fpath.replace('\\', '/')
    prefix_dirs.append(fpath)
    for dr in prefix_dirs:
        dir_path = dr + dir_name
        if os.path.exists(dir_path):
            return dir_path
        else:
            if make:
                logger_show('新建文件夹: {}'.format(dir_path),
                            logger, 'info')
                make_dir(dir_path)
                return dir_path
    raise ValueError('未找到文件夹`{}{}`路径，请检查！'.format(
                     root_dir, dir_name))
    
def find_paths_year(fpath):
    '''根据fpath查找与其相关的带年份后缀的所有路径'''
    file = os.path.basename(fpath)
    fdir = str(Path(fpath).parent)
    fdir = fdir.replace('\\', '/')
    tmp = os.listdir(fdir)
    files = []
    for x in tmp:
        x_, type_ = os.path.splitext(x)
        if (x_[:-5]+type_) == file:
            try:
                _ = int(x_[-4:])
                files.append(fdir+'/'+x)
            except:
                pass
    files.sort(key=lambda x: os.path.splitext(x)[0][-4:])
    files.append(fpath)
    return files

#%%
def load_ccxt_daily(name1, name2, mkt='binance', root_dir=None):
    fdir = find_target_dir('{}/ccxt_{}/'.format(name1, mkt),
                           root_dir=root_dir)
    fpath = fdir + '{}_daily.csv'.format(name2)
    df = load_csv(fpath)
    df['time'] = pd.to_datetime(df['time'])
    # df['time'] = df['time'].apply(lambda x: x-datetime.timedelta(1))
    df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['date'] = df['time']
    df.sort_values('date', ascending=True, inplace=True)
    df.set_index('date', inplace=True)
    return df


def load_daily_btc126(name1, root_dir=None):
    data_dir = find_target_dir('{}/btc126/'.format(name1),
                               root_dir=root_dir)
    fpaths = [data_dir+x for x in os.listdir(data_dir)]
    data = []
    for fpath in fpaths:
        df = load_csv(fpath)
        data.append(df)
    data = pd.concat(data, axis=0)
    data.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'pct%']
    data.sort_values('date', ascending=True, inplace=True)
    data['time'] = data['date'].copy()
    if name1 == 'eth':
        data = data[data['date'] >= '2015-08-07']
    elif name1 == 'btc':
        data = data[data['date'] >= '2010-07-19']
    data.set_index('date', inplace=True)
    data['volume'] = data['volume'].apply(lambda x: eval(''.join(x.split(','))))
    def _get_pct(x):
        try:
            return eval(x.replace('%', ''))
        except:
            return np.nan
    data['pct%'] = data['pct%'].apply(lambda x: _get_pct(x))
    data = data.reindex(columns=['time', 'open', 'high', 'low', 'close',
                                 'volume', 'pct%'])
    return data


def load_daily_qkl123(name1, root_dir=None):
    fdir = find_target_dir('{}/qkl123/'.format(name1),
                           root_dir=root_dir)
    fpath = fdir + '{}-币价走势.csv'.format(name1.upper())
    df = load_csv(fpath).rename(columns={'时间': 'time', '币价': 'close'})
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['date'] = df['time']
    df.sort_values('date', ascending=True, inplace=True)
    df.set_index('date', inplace=True)
    return df


def load_daily_crypto_usdt(name1, name2, mkt='binance',
                           root_dir=None, logger=None):
    assert name1 in ['btc', 'eth'], '`name1`只能是`btc`或`eth`！'
    df0 = load_ccxt_daily(name1, name2, mkt, root_dir)
    df0['data_source'] = 'binance'
    df0['idx'] = range(0, df0.shape[0])
    df1 = load_daily_btc126(name1, root_dir)
    df1['data_source'] = 'btc126'
    df1['idx'] = range(df0.shape[0], df0.shape[0]+df1.shape[0])
    df2 = load_daily_qkl123(name1, root_dir)
    df2['data_source'] = 'qkl123'
    df2['idx'] = range(df0.shape[0]+df1.shape[0], df0.shape[0]+df1.shape[0]+df2.shape[0])
    df = pd.concat((df0, df1, df2), axis=0)
    df.sort_values(['time', 'idx'], inplace=True)
    df.drop_duplicates(subset=['time'], keep='first', inplace=True)
    loss_dates = check_date_loss(df, only_workday=False, del_weekend=False)
    if len(loss_dates) > 0:
        logger_show('{}日线数据有缺失日期：'.format(name1.upper())+','.join(loss_dates),
                    logger, 'warn')
    return df.reindex(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'data_source'])
    

def load_ccxt_minute(name1, name2, minute=15,
                     mkt='binance', root_dir=None,
                     start_time=None, end_time=None):
    fdir = find_target_dir('{}/ccxt_{}/'.format(name1, mkt),
                           root_dir=root_dir)
    fpath = fdir + '{}_{}minute.csv'.format(name2, int(minute))    
    fpaths = find_paths_year(fpath)
    df = load_csvs(fpaths)
    df.sort_values('time', ascending=True, inplace=True)
    if not start_time is None:
        df = df[df['time'] >= start_time]
    if not end_time is None:
        df = df[df['time'] <= end_time]
    df.set_index('time', inplace=True)
    return df

#%%
def load_trade_dates_tushare(exchange='SSE', root_dir=None):
    '''读取交易所交易日历历史数据'''
    fdir = find_target_dir('trade_dates/tushare/', root_dir=root_dir)
    fpath = fdir + '{}.csv'.format(exchange)
    df= load_csv(fpath)
    df.sort_values('date', ascending=True, inplace=True)
    df.drop_duplicates(subset=['date'], keep='last', inplace=True)
    return df

#%%
def load_index_info_tushare(market, root_dir=None):
    '''根据market读取tushare指数基本信息数据'''
    fdir = find_target_dir('index/tushare/index_info/',
                           root_dir=root_dir)
    fpath = fdir + '{}.csv'.format(market)
    return load_csv(fpath, encoding='gbk')


def load_index_info_all_tushare(root_dir=None):
    '''读取tushare全部指数基本信息数据'''
    fdir = find_target_dir('index/tushare/index_info/',
                           root_dir=root_dir)
    mkts = os.listdir(fdir)
    mkts = [x for x in mkts if x.endswith('.csv')]
    df = []
    for mkt in mkts:
        fpath = fdir + mkt
        df.append(load_csv(fpath, encoding='gbk'))
    df = pd.concat(df, axis=0)
    return df


def get_index_code_name_tushare():
    '''获取tushare所有指数代码和对应简称，返回dict'''
    all_indexs = load_index_info_all_tushare().set_index('code')
    code_names = all_indexs['简称'].to_dict()
    return code_names
    

def find_index_code_tushare(info, root_dir=None, logger=None):
    '''传入code查找对应tushare的code'''
    fdir = find_target_dir('index/tushare/',
                           root_dir=root_dir)
    indexs = os.listdir(fdir)
    for x in indexs:
        if info in x:
            return x
    if info in TS_NAME_CODE.keys():
        return TS_NAME_CODE[info]
    code_names = get_index_code_name_tushare()
    for k, v in code_names.items():
        if info in k or info == v:
            return k
    logger_show('未找到`{}`对应指数代码，返回None，请检查输入！'.format(info),
                logger, 'warn')
    return None
    

def load_index_daily_tushare(code, root_dir=None):
    '''读取tushare指数日线数据'''
    ts_code = find_index_code_tushare(code)
    fdir = find_target_dir('index/tushare/{}/'.format(ts_code),
                           root_dir=root_dir)
    fpath = fdir + '{}_daily.csv'.format(ts_code)
    df = load_csv(fpath)
    df.sort_values('date', ascending=True, inplace=True)
    df.drop_duplicates(subset=['date'], keep='last', inplace=True)
    return df


def load_index_daily_basic_tushare(code, root_dir=None):
    '''读取tushare指数日线数据'''
    ts_code = find_index_code_tushare(code)
    fdir = find_target_dir('index/tushare/{}/'.format(ts_code),
                           root_dir=root_dir)
    fpath = fdir + '{}_daily_basic.csv'.format(ts_code)
    df = load_csv(fpath, encoding='gbk')
    df.sort_values('date', ascending=True, inplace=True)
    df.drop_duplicates(subset=['date'], keep='last', inplace=True)
    return df

#%%
def load_astocks_list_tushare(root_dir=None, del_dup=True):
    '''导入A股列表数据'''
    fdir = find_target_dir('stocks/tushare/',
                           root_dir=root_dir)
    df = load_csv(fdir+'astocks_list.csv', encoding='gbk')
    df.sort_values(['code', 'list_date'],
                   ascending=True, inplace=True)
    if del_dup:
        df.drop_duplicates(subset=['code'],
                           keep='last', inplace=True)
    return df


def find_stocks_code_tushare(infos, root_dir=None,
                             logger=None):
    '''查找股票代码，codes为str或list'''
    def _return(cd):
        _cd = {}
        for x in infos:
            if x in cd:
                _cd[x] = cd[x]
            else:
                logger_show('未找到`{}`对应代码'.format(x),
                            logger, 'warn')
                _cd[x] = None
        if _str:
            return list(_cd.values())[0]
        return _cd
    assert isinstance(infos, (str, list, tuple))
    _str = False
    if isinstance(infos, str):
        _str = True
        infos = [infos]
    cd = {}
    for x in infos:
        x_, sure = get_code_ext(x, True)
        if sure:
            cd[x] = x_
    left = [x for x in infos if x not in cd]
    if len(left) == 0:
        return _return(cd)
    df = load_astocks_list_tushare(root_dir=root_dir)
    df['code_'] = df['code'].copy()
    df_ = df[df['code'].isin(infos)].copy()
    cd.update(df_.set_index('code')['code_'].to_dict())
    left = [x for x in infos if x not in cd]
    if len(left) == 0:
        return _return(cd)
    df_ = df[df['symbol'].isin(infos)].copy()
    cd.update(df_.set_index('symbol')['code_'].to_dict())
    left = [x for x in infos if x not in cd]
    if len(left) == 0:
        return _return(cd)
    df_ = df[df['name'].isin(infos)].copy()
    cd.update(df_.set_index('name')['code_'].to_dict())
    return _return(cd)


def _load_stock_daily_tushare(code, ext='', root_dir=None):
    '''读取tushare股票日线数据'''
    assert ext in ['', 'stk', 'basic']
    code = find_stocks_code_tushare(code, root_dir)
    fdir = find_target_dir('stocks/tushare/{}/'.format(code))
    if ext == '':
        fpath = '{}/{}_daily.csv'.format(fdir, code)
    elif ext in ['stk', 'basic']:
        fpath = '{}/{}_daily_{}.csv'.format(fdir, code, ext)
    return load_csv(fpath)


def load_stock_daily_tushare(code, ext='', root_dir=None):
    '''读取tushare股票日线数据'''
    assert isinstance(ext, (str, list, tuple))
    if isinstance(ext, str):
        ext = [ext]
    df = _load_stock_daily_tushare(code, ext[0], root_dir)
    if len(ext) == 0:
        df.sort_values(['code', 'date'], inplace=True)
        return df
    for k in range(1, len(ext)):
        df_ = _load_stock_daily_tushare(code, ext[k], root_dir)
        df = merge_df(df, df_, on=['code', 'date'], how='outer')
    df.sort_values(['code', 'date'], inplace=True)
    return df

#%%
def load_chn_bond_yields(cate='national', root_dir=None):
    '''读取国债收益率历史数据'''
    fdir = find_target_dir('chn_bonds/{}/'.format(cate),
                           root_dir=root_dir)
    fpath = fdir+'chn_{}_bond_rates.csv'.format(cate)
    df = load_csv(fpath, encoding='gbk')
    df.rename(columns={'日期': 'date'}, inplace=True)
    df.sort_values('date', ascending=True, inplace=True)
    df.drop_duplicates(subset=['date'], keep='last', inplace=True)
    return df

#%%
def load_cffex_lhb_future(code, date, root_dir=None):
    '''读取中金所期货龙虎榜数据'''
    fdir = find_target_dir('futures/cffex/lhb/{}/'.format(code),
                           root_dir=root_dir)
    date = date_reformat(date, '')
    fpath = '{}{}{}.csv'.format(fdir, code, date)
    df = load_text_multi(fpath, encoding='gbk')
    return df

#%%
def load_futures_info_tushare(exchange=None, root_dir=None):
    '''
    | 读取期货基本信息数据
    | exchange指定交易所，不指定则读取所有的
    '''
    pdir = find_target_dir('futures/tushare/futures_info/',
                           root_dir=root_dir)
    if isnull(exchange):
        fpaths = os.listdir(pdir)
        fpaths = [pdir+x for x in fpaths if x[-4:] == '.csv']
    else:
        fpaths = [pdir+exchange+'.csv']
    df = load_csvs(fpaths, encoding='gbk')
    return df


def find_futures_exchange_tushare(infos, root_dir=None,
                                  logger=None):
    '''根据期货合约代码或名称查找所在交易所，codes为str或list'''
    def _return(ex):
        _ex = {}
        for k, v in codes_.items():
            if k in ex:
                _ex[k] = ex[k]
            elif v in ex:
                _ex[k] = ex[v]
            else:
                logger_show('未找到`{}`对应交易所'.format(k),
                            logger, 'warn')
                _ex[k] = None
        if _str:
            return list(_ex.values())[0]
        return _ex
    assert isinstance(infos, (str, list, tuple))
    _str = False
    if isinstance(infos, str):
        _str = True
        infos = [infos]
    codes_ = {x: TS_NAME_CODE[x] if x in TS_NAME_CODE else x for x in infos}
    infos = [TS_NAME_CODE[x] if x in TS_NAME_CODE else x for x in infos]
    ex = {}
    ex_ = {'.CFX': 'CFFEX', '.ZCE': 'CZCE', '.INE': 'INE',
           '.DCE': 'DCE', '.SHF': 'SHFE'}
    ex.update({x: ex_['.'+x.split('.')[-1]] for x in infos if '.'+x.split('.')[-1] in ex_.keys()})
    left = [x for x in infos if x not in ex]
    if len(left) == 0:
        return _return(ex)
    df = load_futures_info_tushare(root_dir=root_dir)
    df_ = df[df['code'].isin(infos)].copy()
    ex.update(df_.set_index('code')['交易市场'].to_dict())
    left = [x for x in infos if x not in ex]
    if len(left) == 0:
        return _return(ex)
    df['code'] = df['code'].apply(lambda x: x.split('.')[0])
    df_ = df[df['code'].isin(infos)].copy()
    ex.update(df_.set_index('code')['交易市场'].to_dict())
    left = [x for x in infos if x not in ex]
    if len(left) == 0:
        return _return(ex)
    df_ = df[df['简称'].isin(infos)].copy()
    ex.update(df_.set_index('简称')['交易市场'].to_dict())
    return _return(ex)


def find_futures_code_tushare(infos, root_dir=None,
                              logger=None):
    '''查找期货合约代码，codes为str或list'''
    def _return(cd):
        _cd = {}
        for x in infos:
            if x in cd:
                _cd[x] = cd[x]
            else:
                logger_show('未找到`{}`对应代码'.format(x),
                            logger, 'warn')
                _cd[x] = None
        if _str:
            return list(_cd.values())[0]
        return _cd
    assert isinstance(infos, (str, list, tuple))
    _str = False
    if isinstance(infos, str):
        _str = True
        infos = [infos]
    cd = {x: TS_NAME_CODE[x] for x in infos if x in TS_NAME_CODE}
    left = [x for x in infos if x not in cd]
    if len(left) == 0:
        return _return(cd)
    df = load_futures_info_tushare(root_dir=root_dir)
    df['code_'] = df['code'].copy()
    df_ = df[df['code'].isin(infos)].copy()
    cd.update(df_.set_index('code')['code_'].to_dict())
    left = [x for x in infos if x not in cd]
    if len(left) == 0:
        return _return(cd)
    df['code'] = df['code'].apply(lambda x: x.split('.')[0])
    df_ = df[df['code'].isin(infos)].copy()
    cd.update(df_.set_index('code')['code_'].to_dict())
    left = [x for x in infos if x not in cd]
    if len(left) == 0:
        return _return(cd)
    df_ = df[df['简称'].isin(infos)].copy()
    cd.update(df_.set_index('简称')['code_'].to_dict())
    return _return(cd)
        
    
def load_future_daily_ex_tushare(exchange, root_dir=None):
    '''读取tushare交易所期货日线数据'''
    fdir = find_target_dir('futures/tushare/futures_daily/',
                           root_dir=root_dir)
    fpath = fdir + exchange + '.csv'
    fpaths = find_paths_year(fpath)
    df = load_csvs(fpaths, encoding='gbk')
    df.sort_values(['date', 'code'], ascending=True,
                   inplace=True)
    return df


def load_future_daily_tushare(code, root_dir=None, logger=None):
    '''读取tushare期货日线数据，code为tushare代码'''
    fdir = find_target_dir('futures/tushare/', root_dir=root_dir)
    files = os.listdir(fdir)
    if code in files:
        fpath = fdir + '{}/{}_daily.csv'.format(code, code)
        return load_csv(fpath, encoding='gbk')
    else:
        exchange = find_futures_exchange_tushare(code, root_dir, logger)
        df = load_future_daily_ex_tushare(exchange, root_dir)
        code = find_futures_code_tushare(code, root_dir, logger)
        return df[df['code'] == code]
    
#%%
def load_options_info_tushare(exchange, root_dir=None):
    '''读取tushare期权基本信息数据'''
    fdir = find_target_dir('options/tushare/options_info/',
                           root_dir=root_dir)
    fpath = fdir + exchange + '.csv'
    return load_csv(fpath, encoding='gbk')


def load_options_daily_ex_tushare(exchange, root_dir=None):
    '''读取tushare交易所期权日线数据'''
    fdir = find_target_dir('options/tushare/options_daily/',
                           root_dir=root_dir)
    fpath = fdir + exchange + '.csv'
    fpaths = find_paths_year(fpath)
    df = load_csvs(fpaths)
    df.sort_values(['date', 'code'], ascending=True,
                   inplace=True)
    return df

#%%
def load_fund_daily_tushare(code, fq='qfq', root_dir=None):
    '''读取tushare基金日线数据'''
    assert fq in ['', 'qfq', 'hfq']
    if fq != '':
        fq = '_'+fq
    fdir = find_target_dir('fund/tushare/{}/'.format(code),
                           root_dir=root_dir)
    fpath = '{}{}_daily{}.csv'.format(fdir, code, fq)
    df = load_csv(fpath)
    return df

#%%
if __name__ == '__main__':
    import time

    strt_tm = time.time()
    
    #%%
    df_eth = load_daily_crypto_usdt('eth', 'eth_usdt')
    df_btc = load_daily_crypto_usdt('btc', 'btc_usdt')
    df_eth_15m = load_ccxt_minute('eth', 'eth_usdt')
    df_btc_5m = load_ccxt_minute('btc', 'btc_usdt', 5)
    df_btc_1m = load_ccxt_minute('btc', 'btc_usdt', 1,
                                  start_time='2022-02-01 05:00:00',
                                  end_time='2022-06-09 14:00:00')

    #%%
    df_chn_bonds = load_chn_bond_yields()
    df_chn_bonds_local = load_chn_bond_yields('local')
    
    #%%
    df_trade_dates = load_trade_dates_tushare()
    
    #%%
    df_a = load_astocks_list_tushare()
    
    #%%
    df_cffex = load_cffex_lhb_future('IF', '2022-06-10')
    
    #%%
    df = load_index_daily_tushare('中证1000')
    df_ = load_index_daily_basic_tushare('沪深300')
    
    df_sse = load_index_info_tushare('SSE')
    
    df_sh = load_index_daily_tushare('000001.SH')
    df_sh_basic = load_index_daily_basic_tushare('000001.SH')
    df_hs300 = load_index_daily_tushare('000300.SH')
    df_hs300_basic = load_index_daily_basic_tushare('000300.SH')
    df_hs300_ = load_index_daily_tushare('399300.SZ')
    df_zz500 = load_index_daily_tushare('000905.SH')
    df_zz500_basic = load_index_daily_basic_tushare('000905.SH')

    #%%
    df_if = load_future_daily_tushare('IF.CFX')
    
    #%%
    df_opt_sse = load_options_daily_ex_tushare('SSE')

    #%%
    print('used time: {}s.'.format(round(time.time()-strt_tm, 6)))
