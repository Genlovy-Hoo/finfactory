# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd

from dramkit import simple_logger
from dramkit.gentools import isnull, merge_df
from dramkit.gentools import get_preval_func_cond
from dramkit.gentools import get_con_start_end
from dramkit.datetimetools import today_date
from dramkit.iotools import make_dir

from finfactory.fintools.fintools import boll, cci
from finfactory.fintools.utils_chn import get_trade_dates
from finfactory.fintools.utils_chn import get_num_trade_dates

from finfactory.options_trader.basic_config import OptsConfig
from finfactory.options_trader.basic_config import ETFsConfig
from finfactory.options_trader.asset_account import AssetAccount

#%%
class OptsTrader(object):
    '''交易策略'''
    
    def __init__(self, start_date='2017-01-01', end_date=None,
                 account=None, logger=None):
        '''
        | start_date和end_date为回测起始日期
        | account: AssetAccount实例
        '''
        
        self.logger = simple_logger() if isnull(logger) else logger
        
        # 交易日期
        self.start_date = start_date
        self.end_date = today_date() if isnull(end_date) else end_date
        self.trade_dates = get_trade_dates(self.start_date,
                                           self.end_date)
        
        # 资产账户
        self.account = AssetAccount() if isnull(account) else account
        
        # 单位数量手续费
        self.__feeBuy = 12
        self.__feeSel = 0
        
        # 账户资产相关设置
        self.__init_money = 20000 # 初始资金
        
        # 交易控制参数
        self.open_min_leftdays = 40 # 开新仓合约剩余最少交易天数
        self.add_min_leftdays = 15 # 加仓合约剩余最少交易天数
        
    @property
    def feeBuy(self):
        return self.__feeBuy
    
    @property
    def feeSel(self):
        return self.__feeSel
        
    @property
    def init_money(self):
        '''初始资金'''
        return self.__init_money
    
    @staticmethod
    def get_opt_info(date, code, opts_cfg):
        '''根据日期和代码获取合约信息，返回dict'''
        opt = opts_cfg.opts_daily_sh.loc[date, :].copy()
        opt = opt[opt['合约代码'] == code].reset_index()
        opt_info = opt.to_dict('index')[0]
        return opt_info
    
    @staticmethod
    def get_trade_net_cash(Price, Vol, OpnCls, RgtDty, fee,
                           opt_unit=10000, base_unit=10000):
        '''计算交易资金净流量'''
        p = Price * opt_unit / base_unit # 分红处理
        if RgtDty == '权利':
            if OpnCls == '开':
                return -1 * Vol * (p + fee)
            else:
                return Vol * (p + fee)
        else:
            # 注：义务方开仓净入资金是虚拟的
            if OpnCls == '开':
                return Vol * (p - fee)
            else:
                return -1 * Vol * (p + fee)
        
    def cashInOut(self, act_time, vCash, InOut):
        '''
        账户入金和提现操作
        '''
        if InOut == 'in':
            self.logger.info(f'{act_time} 入金 {vCash}.')
            record = [act_time, vCash, np.nan]
        elif InOut == 'out':
            self.logger.info(f'{act_time} 提现 {vCash}.')
            record = [act_time, np.nan, vCash]
        else:
            raise ValueError('InOut必须设置为`in`或`out`！')
        record = pd.DataFrame([record], columns=self.account.cash_flow_cols)
        cash_flow = pd.concat((self.account.cash_flow, record), axis=0)
        cash_flow = cash_flow.reindex(columns=self.account.cash_flow_cols)
        cash_flow.reset_index(drop=True, inplace=True)
        self.account.cash_flow = cash_flow
        if not isnull(self.account.cash_flow_path):
            cash_flow.to_csv(self.account.cash_flow_path, index=None)
            
    def get_hold_cash(self):
        '''获取账户可用资金'''
        if self.account.asset_records.shape[0] == 0:
            return 0
        return self.account.asset_records['现金'].iloc[-1]
        
    def act_trade(self, plan, opts_cfg):
        '''根据交易计划plan执行交易（更新账户交易记录）'''
        opt_info = self.get_opt_info(plan['日期'], plan['合约代码'], opts_cfg)
        Plow, Phigh = opt_info['low'], opt_info['high']
        if Plow*10000 <= plan['成交均价'] <= Phigh*10000:
            trade_record = plan.copy()
            fee = self.feeBuy if plan['开or平'] == '开' else self.feeSel
            trade_net_cash = self.get_trade_net_cash(plan['成交均价'],
                    plan['成交数量'], plan['开or平'], plan['权利or义务'], fee=fee)
            # if trade_net_cash < 0 and abs(trade_net_cash) > self.get_hold_cash():
            #     self.cashInOut(plan['日期']+' 09:00:01', abs(trade_net_cash), 'in')
            trade_record.update({'单位数量手续费': fee,
                                 '交易净入': trade_net_cash,
                                 '合约单位': 10000, '基准单位': 10000})
            trade_record = pd.DataFrame.from_dict(trade_record, 'index')
            trade_record = trade_record.transpose()
            trade_records = pd.concat(
                            (self.account.trade_records, trade_record), axis=0)
            trade_records = trade_records.reindex(
                            columns=self.account.trade_record_cols)
            self.account.trade_records = trade_records
            # 交易成功
            return True
        else:
            # 交易失败
            return False
        
    def settle(self, opts_cfg, date_last=None):
        '''账户归并结算'''
        self.account.opts_settle_his(opts_cfg, stop_date=date_last)
        self.account.asset_settle(opts_cfg, stop_date=date_last)
        self.account.get_hold_info(opts_cfg, stop_date=date_last)
        
    def hold_none(self):
        '''判断是否空仓'''
        if self.account.hold_info_now['多头'] == 0 and \
                                      self.account.hold_info_now['空头'] == 0:
            return True
        return False
    
    @staticmethod
    def opts_etfs_process(opts_cfg, etfs_cfg):
        '''（自定义）标的证券数据预处理'''
        
        etf = etfs_cfg.daily['510050.SH']
        
        # 布林带
        df_boll = boll(etf['close'])
        etf = merge_df(etf, df_boll, same_keep='left', how='left',
                      left_index=True, right_index=True)
        etf['more_up'] = etf[['high', 'boll_up']].apply(lambda x: 
                        1 if x['high'] >= x['boll_up'] else 0, axis=1) # 上穿
        etf['less_low'] = etf[['low', 'boll_low']].apply(lambda x:
                       -1 if x['low'] <= x['boll_low'] else 0, axis=1) # 下穿
        etf['label_boll'] = etf['more_up'] + etf['less_low']
        etf.drop(['more_up', 'less_low'], axis=1, inplace=True)
        etf['label_boll_pre'] = get_preval_func_cond(etf, 'label_boll',
                                              'label_boll', lambda x: x != 0)
        
        # CCI
        etf['cci'] = cci(etf)
        etf['cci_100'] = etf['cci'].apply(lambda x: 1 if x > 100 else \
                                                      (-1 if x < -100 else 0))
        strt_ends_1 = get_con_start_end(etf['cci_100'], lambda x: x == -1)
        strt_ends1 = get_con_start_end(etf['cci_100'], lambda x: x == 1)
        etf['cci_100_'] = 0
        for start, end in strt_ends_1:
            if end+1 < etf.shape[0]:
                etf.loc[etf.index[end+1], 'cci_100_'] = -1
        for start, end in strt_ends1:
            if end+1 < etf.shape[0]:
                etf.loc[etf.index[end+1], 'cci_100_'] = 1
        
        etf.set_index('date', inplace=True)
        
        # etf数据更新
        etfs_cfg.daily['510050.SH'] = etf
        
        # 合约标的价格
        opts = opts_cfg.opts_daily_sh.set_index('日期')
        close_etf = etf[['close']].rename(columns={'close': 'close_etf'})
        opts = pd.merge(opts, close_etf, how='left', left_index=True,
                        right_index=True)
        
        # 合约信息
        opts_info = opts_cfg.opts_info
        opts = pd.merge(opts.reset_index(), opts_info,
                        how='left', on='合约代码')
        opts.rename(columns={'index': '日期'}, inplace=True)
        
        # 合约数据更新
        opts_cfg.opts_daily_sh = opts.set_index('日期')
        
    @staticmethod
    def get_price_most_close_opts(opts):
        '''获取行权价跟标的证券收盘价价格最接近的合约'''
        opts['行权_当前价差'] = abs(opts['行权价格'] - opts['close_etf'])
        abs_min_price_diff = opts['行权_当前价差'].min()
        opts = opts[opts['行权_当前价差'] == abs_min_price_diff].copy()
        return opts
    
    @staticmethod
    def get_opts_by_price_leftdays(opts, min_leftdays=40, opts_cfg=None):
        '''获取行权价跟标的证券最接近且剩余交易日大于等于且最接近min_leftdays的合约'''
        close_opts = self.get_price_most_close_opts(opts)
        if isnull(opts_cfg):
            trade_dates_df_path = None
        else:
            trade_dates_df_path = opts_cfg.trade_dates
        close_opts['当前日'] = close_opts.index
        close_opts['剩余交易日'] = close_opts[['当前日', '最后交易日期']].apply(
                    lambda x: get_num_trade_dates(x['当前日'], x['最后交易日期'],
                                                  trade_dates_df_path), axis=1)
        close_opts = close_opts[close_opts['剩余交易日'] >= min_leftdays]
        close_opts = close_opts[close_opts['剩余交易日'] == \
                                                close_opts['剩余交易日'].min()]
        close_opts = close_opts[close_opts['开始交易日期'] == \
                                               close_opts['开始交易日期'].max()]
        return close_opts
        
    def get_act_plans(self, date, opts_cfg, etfs_cfg):
        '''
        | （自定义）操作计划
        | 返回[(时间, 代码, 开or平, 权利or义务, 数量, 价格), ...]
        '''
        
        etf = etfs_cfg.daily['510050.SH'].copy()
        opts = opts_cfg.opts_daily_sh.loc[date, :].copy()
        
        # 若当前空仓，则根据ETF布林带位置决定多空开仓比并选定合适的合约
        if self.hold_none():
            close_opts = self.get_opts_by_price_leftdays(opts,
                                            self.open_min_leftdays, opts_cfg)
            
            # 前一个布林带上下轨穿越信号
            pre_boll_label = etf.loc[date, 'label_boll_pre']
            
            if pre_boll_label == 1:
                # 做空
                trade_opt = close_opts[close_opts['合约方向'] == '空']                
            elif pre_boll_label == -1:
                # 做多
                trade_opt = close_opts[close_opts['合约方向'] == '空']
                
            # 交易计划: {日期, 时间, 代码, 开or平, 权利or义务, 价格, 数量}
            act_time = date + ' 14:59:59'
            code = trade_opt['合约代码'].iloc[0]
            OpnCls = '开'
            RgtDty = '权利'
            price = trade_opt['close'].iloc[0] * 10000
            if price >= 1000:
                vol = 1
            else:
                vol = 1000 // price
            
            return [{'日期': date, '时间': act_time, '合约代码': code,
                     '开or平': OpnCls, '权利or义务': RgtDty, '成交数量': vol,
                     '成交均价': price}]
        else:
            hold_last = self.account.hold_last.copy()
            hold_doRgt = hold_last[(hold_last['合约方向'] == '多') & \
                                   (hold_last['权利or义务'] == '权利')].copy()
            hold_ksRgt = hold_last[(hold_last['合约方向'] == '空') & \
                                   (hold_last['权利or义务'] == '权利')].copy()
            doRgtCost = hold_doRgt['总成本'].sum()
            ksRgtCost = hold_ksRgt['总成本'].sum()
            # doRgtGainPct = 
        
    def backtest(self, opts_cfg, etfs_cfg):
        '''回测过程'''
        
        self.logger.info('回测账户初始化......................................')
        
        # ETF和合约历史数据预处理
        self.opts_etfs_process(opts_cfg, etfs_cfg)
        
        # # 初始入金
        # self.cashInOut(self.start_date+' 09:00:01', self.init_money, 'in')
        
        # 交易前账户归并结算
        self.settle(opts_cfg, date_last=self.start_date)
        
        k = 0
        while k < len(self.trade_dates):
            date = self.trade_dates[k]
            self.logger.info(f'回测，当前日期：{date}..........................')
            
            # 交易计划
            plans = self.get_act_plans(date, opts_cfg, etfs_cfg)
            
            # 交易执行
            for plan in plans:
                # 检查账户资金是否够用，不够用需入金
                fee = self.feeBuy if plan['开or平'] == '开' else self.feeSel
                trade_net_cash = self.get_trade_net_cash(plan['成交均价'],
                        plan['成交数量'], plan['开or平'], plan['权利or义务'],
                        fee=fee)
                if trade_net_cash < 0 and \
                                   abs(trade_net_cash) > self.get_hold_cash():
                    self.cashInOut(plan['日期']+' 09:00:01',
                                               abs(trade_net_cash)+100, 'in')
                tradeOK = self.act_trade(plan, opts_cfg)
            if not isnull(self.account.trade_records_path):
                self.account.trade_records.to_csv(
                                 self.account.trade_records_path, index=None)
                
            # 交易后账户归并结算
            self.settle(opts_cfg, date_last=date)
            
            k += 1
            
#%%
if __name__ == '__main__':
    strt_tm = time.time()
    
    #%%
    opts_cfg = OptsConfig()
    etfs_cfg = ETFsConfig()
    
    # 资产账户信息
    account_id = '00000001'
    account_name = 'TargetMultiple'
    stratety_dir = '../../output/opts_strategy/TargetMultiple/'
    if not os.path.exists(stratety_dir):
        make_dir(stratety_dir)
    opts_settle_path = stratety_dir + 'opts_settle.csv'
    trade_records_path = stratety_dir + 'opts_trade_record.csv'
    cash_flow_path = stratety_dir + 'cash_flow.csv'
    asset_settle_path = stratety_dir + 'asset_settle.csv'
    logger = None
    
    # 清空资产账户相关文件
    fpaths = [cash_flow_path, trade_records_path, asset_settle_path,
              opts_settle_path]
    for fpath in fpaths:
        if not isnull(fpath) and os.path.exists(fpath):
            os.remove(fpath)
    
    # 资产账户
    account = AssetAccount(account_id=account_id, account_name=account_name,
                           opts_settle_path=opts_settle_path,
                           trade_records_path=trade_records_path,
                           cash_flow_path=cash_flow_path,
                           asset_settle_path=asset_settle_path,
                           logger=logger)
    
    # 策略
    self = OptsTrader(account=account)
    # self.backtest(opts_cfg, etfs_cfg)
    
    #%%
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    