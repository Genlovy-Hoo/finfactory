# -*- coding: utf-8 -*-


import pandas as pd
import finfactory.load_his_data as lhd


class OptsConfig(object):
    '''期权基本行情信息配置'''
    
    def __init__(self, root_dir=None):
        '''
        | 基本行情数据信息读取准备
        | 交易日期、合约信息、日线行情默认存档数据为tushare格式
        | 分钟行情存放数据格式为聚宽
        '''
        
        # 数据根目录
        self.root_dir = root_dir
        
        # 交易日期
        self.trade_dates_sh = lhd.load_trade_dates_tushare('SSE', self.root_dir)
        self.trade_dates_sz = lhd.load_trade_dates_tushare('SZSE', self.root_dir)
        # 默认交易日期为上交所日期
        self.trade_dates = self.trade_dates_sh
        
        # 合约信息
        opts_info_sh = lhd.load_options_info_tushare('SSE', self.root_dir)
        opts_info_sz = lhd.load_options_info_tushare('SZSE', self.root_dir)
        self.opts_info_sh = self.handle_opts_info(opts_info_sh)
        self.opts_info_sz = self.handle_opts_info(opts_info_sz)
        # 合约信息合并
        self.opts_info = pd.concat((self.opts_info_sh, self.opts_info_sz),
                                   axis=0).reset_index(drop=True)
            
        # 日线行情
        opts_daily_sh = lhd.load_options_daily_ex_tushare('SSE', self.root_dir)
        opts_daily_sz = lhd.load_options_daily_ex_tushare('SZSE', self.root_dir)
        self.opts_daily_sh = self.handle_opts_daily(opts_daily_sh)
        self.opts_daily_sz = self.handle_opts_daily(opts_daily_sz)
        # 日线行情合并
        opts_daily = pd.concat((self.opts_daily_sh, self.opts_daily_sz),
                               axis=0)
        opts_daily.sort_values(['合约代码', '日期'],
                               ascending=True, inplace=True)
        opts_daily.reset_index(drop=True, inplace=True)
        self.opts_daily = opts_daily
        
        # 分钟行情
        self.opts_minute_dir = lhd.find_target_dir(
                                'options/joinquant/options_minute/',
                                self.root_dir)
        
    @staticmethod
    def handle_opts_info(df):
        df = df.rename(columns={'code': '合约代码', '名称': '合约名称'})
        df['合约方向'] = df['合约名称'].apply(
                       lambda x: '多' if '认购' in x else '空')
        return df
        
    @staticmethod
    def handle_opts_daily(df):
        df = df.rename(columns={'code': '合约代码', 'date': '日期'})
        return df
    
    
class ETFsConfig(object):
    '''期权对应ETF基本行情信息配置'''
    
    def __init__(self, root_dir=None, fq='qfq'):
        '''
        | ETF基本行情信息读取
        | 日线行情默认存档数据为tushare格式
        '''
        
        # 数据根目录
        self.root_dir = root_dir
        self.fq = fq
            
        # ETF日线行情
        self.daily = {}
        self.daily['510050.SH'] = lhd.load_fund_daily_tushare(
                                    '510050.SH', self.fq, self.root_dir)
        self.daily['510050.SH'] = lhd.load_fund_daily_tushare(
                                    '510300.SH', self.fq, self.root_dir)
        self.daily['510050.SH'] = lhd.load_fund_daily_tushare(
                                    '159919.SZ', self.fq, self.root_dir)




        