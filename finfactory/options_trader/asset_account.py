# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
from random import randint
from dramkit import load_csv, isnull
from dramkit import logger_show, simple_logger
from dramkit.gentools import merge_df, x_div_y
from dramkit.datetimetools import today_date
from finfactory.fintools.utils_chn import get_trade_dates
from finfactory.fintools.utils_gains import cal_gain_pct, get_gains
from finfactory.fintools.utils_gains import plot_gain_act, plot_gain_prod
from finfactory.fintools.utils_chn import get_next_nth_trade_date as next_date
from finfactory.options_trader.basic_config import OptsConfig    

#%%
class AssetAccount(object):
    '''
    账户资产管理
    
    | 主要功能和步骤？
    | 通过初始资产和交易记录归并每日合约资产情况？
    | 通过现金流和合约持有情况进行资产归并？
    
    TODO
    ----
    - 每日资金占用量义务方按平仓资金计算
    - 每日资金占用量新增按上日持仓价值和当日资金转入转出计算
    
    
    '''
    
    def __init__(self, account_id=None, account_name=None,
                 trade_records_path=None, opts_settle_path=None,
                 cash_flow_path=None, asset_settle_path=None,
                 logger=None):
        '''
        Parameters
        ----------
        account_id : str
            账户唯一标识
        account_name : str
            账户别称
        trade_records_path : str
            | 历史交易记录存放csv路径，包含列：
            | ['日期', '时间', '合约代码', '开or平', '权利or义务', '成交数量',
               '成交均价', '单位数量手续费', '交易净入', '合约单位', '基准单位']
        opts_settle_path : str
            | 合约每日持仓归并存放csv路径，包含列：
            | ['日期', '合约代码', '合约名称', '合约方向', '权利or义务', '持仓数量',
               '合约价格', '持仓价值', '持仓成本', '总成本', '持仓盈亏%']
        cash_flow_path : str
            | 现金转入转出记录csv路径，包含列：
            | ['时间', '转入', '转出']
        asset_settle_path : str
            | 每日资产结算记录存放csv路径，包含列：
            | ['日期', '转入', '转出', '交易净入', '合约总价值', '现金', '资产总值']
        '''
        
        #%%
        # 基本设置-------------------------------------------------------------
        self.logger = simple_logger() if logger is None else logger
        
        #%%
        # 账户标识-------------------------------------------------------------
        # 默认将账户id初始化为8位数字文本
        if isnull(account_id):
            logger_show('账户id：随机分配8位数字id。', self.logger)
            self.__account_id = str(randint(0, 99999999)).zfill(8)
        else:
            self.set_account_id(account_id)            
        self.__account_name = account_name
        
        #%%
        # 交易和资产-----------------------------------------------------------
        self.__trade_records_path = trade_records_path
        self.__opts_settle_path = opts_settle_path
        self.__cash_flow_path = cash_flow_path
        self.__asset_settle_path = asset_settle_path
        
        self.__trade_record_cols = ['日期', '时间', '合约代码', '开or平',
                                    '权利or义务', '成交数量', '成交均价',
                                    '单位数量手续费', '交易净入', '合约单位',
                                    '基准单位']
        self.__opts_settle_cols = ['日期', '合约代码', '合约名称', '合约方向',
                                   '权利or义务', '持仓数量', '合约价格',
                                   '持仓价值', '持仓成本', '总成本', '持仓盈亏%']
        self.__cash_flow_cols = ['时间', '转入', '转出']
        
        # 导入历史交易记录
        self.trade_records = self.load_records(self.trade_records_path,
                                               self.trade_record_cols)
        
        # 导入现金转入转出记录
        self.cash_flow = self.load_records(self.cash_flow_path,
                                           self.cash_flow_cols)
        
    #%%
    # 账户标识-----------------------------------------------------------------
    def set_account_id(self, account_id):
        '''设置账户id'''
        # 手动设置id超过8位时抛出警告但是保留设置
        if len(str(account_id)) > 8:
            logger_show('检测到id超过8位: ' + str(account_id) + '！',
                        self.logger, 'warn')
        self.__account_id = str(account_id).zfill(8)
    
    @property
    def account_id(self):
        '''账户id'''
        return self.__account_id
    
    def set_account_name(self, account_name):
        '''设置账户名称'''
        self.__account_name = account_name
    
    @property
    def account_name(self):
        '''账户名称'''
        # if self.__account_name is None:
        #     logger_show('账户名称为None，可通过set_account_name方法设置。', self.logger)
        return self.__account_name
        
    #%%
    # 交易和资产---------------------------------------------------------------    
    @property
    def trade_records_path(self):
        '''合约交易记录csv保存路径'''
        return self.__trade_records_path
    
    @property
    def opts_settle_path(self):
        '''合约每日持仓归并存放csv路径'''
        return self.__opts_settle_path
    
    @property
    def cash_flow_path(self):
        '''现金转入转出记录csv路径'''
        return self.__cash_flow_path
    
    @property
    def asset_settle_path(self):
        '''每日资产结算记录存放csv路径'''
        return self.__asset_settle_path
    
    @property
    def trade_record_cols(self):
        '''交易历史记录默认列名列表'''
        return self.__trade_record_cols
    
    @property
    def opts_settle_cols(self):
        '''历史持仓归并记录默认列名列表'''
        return self.__opts_settle_cols
    
    @property
    def cash_flow_cols(self):
        '''资金转入转出记录默认列名列表'''
        return self.__cash_flow_cols
    
    #%%
    # 历史交易记录--------------------------------------------------------------
    @staticmethod
    def load_records(records_path, default_cols):
        '''导入历史记录数据，若文件不存在，则返回包含default_cols列名的空df'''
        if isnull(records_path) or (isinstance(records_path, str) and \
                                    not os.path.exists(records_path)):
            records = pd.DataFrame(columns=default_cols)
        else:
            records = load_csv(records_path, encoding='gbk')
        return records
    
    def check_trade_records(self, opts_cfg, last_date=None):
        '''检查交易记录数据是否存在错误（时间、代码、开平仓数量等）'''
        self.check_buy_sel(self.trade_records, last_date=last_date,
                           logger=self.logger)
        self.check_optns_code(self.trade_records, opts_cfg, logger=self.logger)
        self.check_optns_time(self.trade_records, opts_cfg, logger=self.logger)
    
    @staticmethod
    def check_buy_sel(trade_records, last_date=None, logger=None):
        '''
        | 根据开平仓数量检查交易记录是否存在错误
        | trade_records，须包含下面几列:
        |     ['日期', '时间', '合约代码', '开or平', '权利or义务', '成交数量']
        | 可指定只检查last_date及其之前日期的数据，last_date格式如'2020-09-11'
        '''
        
        if last_date is not None:
            df = trade_records[trade_records['日期'] <= last_date].copy()
        else:
            df = trade_records.copy()
        
        # 打印交易记录起止时间
        logger_show(f'交易记录开始时间: {df["时间"].min()}', logger)
        logger_show(f'交易记录结束时间: {df["时间"].max()}\n', logger)
        
        # 检查开仓和平仓的数量是否相等
        df['n'] = df['开or平'].apply(lambda x: 1 if x == '开' else -1)
        df['n'] = df['n'] * df['成交数量']
        df_n = df.groupby('合约代码').sum()['n']
        df_error = pd.DataFrame(df_n[df_n != 0])
        df_error.columns = ['n_开-平']
        if df_error.shape[0] > 0:
            logger_show(f'开仓和平仓数量不等的合约: \n{df_error}\n', logger)
        else:
            logger_show('开仓和平仓数量不等的合约: 无\n', logger)
        
        # 检查权利方开仓和权利方平仓的数量是否相等
        df['n_right'] = df[['开or平', '权利or义务']].apply(
           lambda x: 1 if x['开or平'] == '开' and x['权利or义务'] == '权利' \
           else (-1 if x['开or平'] == '平' and x['权利or义务'] == '权利' else 0),
           axis=1)
        df['n_right'] = df['n_right'] * df['成交数量']
        df_n = df.groupby('合约代码').sum()['n_right']
        df_error = pd.DataFrame(df_n[df_n != 0])
        df_error.columns = ['n_权利开-权利平']
        if df_error.shape[0] > 0:
            logger_show(f'权利方开仓和权利方平仓数量不等的合约: \n{df_error}\n', logger)
        else:
            logger_show('权利方开仓和权利方平仓数量不等的合约: 无\n', logger)
        
        # 检查义务方开仓和义务方平仓的数量是否相等
        df['n_duty'] = df[['开or平', '权利or义务']].apply(
            lambda x: -1 if x['开or平'] == '开' and x['权利or义务'] == '义务' \
            else (1 if x['开or平'] == '平' and x['权利or义务'] == '义务' else 0),
            axis=1)
        df['n_duty'] = df['n_duty']*df['成交数量']
        df_n = df.groupby('合约代码').sum()['n_duty']
        df_error = pd.DataFrame(df_n[df_n != 0])
        df_error.columns = ['n_义务开-义务平']
        if df_error.shape[0] > 0:
            logger_show(f'义务方开仓和义务方平仓数量不等的合约: \n{df_error}\n', logger)
        else:
            logger_show('义务方开仓和义务方平仓数量不等的合约: 无\n', logger)
    
    @staticmethod
    def check_optns_code(trade_records, opts_cfg, logger=None):
        '''
        | 检查交易记录中的合约代码是否存在错误
        | trade_records和opts_cfg.opts_info中须包含'合约代码'列
        '''
        
        codes_all = list(opts_cfg.opts_info['合约代码'].unique())        
        codes = list(trade_records['合约代码'].unique())
        codes_error = [x for x in codes if x not in codes_all]
        
        if len(codes_error) > 0:
            logger_show(f'可能存在错误的合约代码: \n{codes_error}\n', logger)
        else:
            logger_show('可能存在错误的合约代码: 无\n', logger)
               
    @staticmethod
    def check_optns_time(trade_records, opts_cfg, logger=None):
        '''
        | 检查交易记录中的交易时间是否存在错误
        | trade_records需包含['日期', '时间列']，
        | opts_cfg.trade_dates须包含['date', 'is_open']列
        '''
        
        dates_all = opts_cfg.trade_dates.copy()
        dates_all = opts_cfg.trade_dates[dates_all['is_open'] == 1].copy()
        dates_all = list(dates_all['date'].unique())
        dates = list(trade_records['日期'].unique())
        times_error = [x for x in dates if x not in dates_all]
        
        times = list(trade_records['时间'].unique())
        times_error += [x for x in times if x[-8:] < '09:24:55']
        times_error += [x for x in times if x[-8:] > '15:00:03']
        times_error += [x for x in times if '11:30:03' < x[-8:] < '12:59:55']
        
        if len(times_error) > 0:
            logger_show(f'可能存在错误的交易时间: \n{times_error}\n', logger)
        else:
            logger_show('可能存在错误的交易时间: 无\n', logger)
    
    @staticmethod
    def get_cash_folw_daily(cash_flow, opts_cfg):
        '''资金转入转出按日汇总'''
        cash_flow_daily = cash_flow.copy()
        cash_flow_daily['日期'] = cash_flow_daily['时间'].apply(
                lambda x: next_date(str(x)[0:10], 1, opts_cfg.trade_dates) \
                          if str(x)[-8:] > '15:00:00' else str(x)[0:10])
        cash_flow_daily = cash_flow_daily.groupby('日期', as_index=False).sum()
        return cash_flow_daily.reindex(columns=['日期', '转入', '转出'])
        
    #%%
    # 历史持仓归并--------------------------------------------------------------
    @staticmethod
    def load_opts_settle(opts_settle_path, default_cols):
        '''导入历史每日合约持仓归并记录'''
        if isnull(opts_settle_path) or \
                                (isinstance(opts_settle_path, str) and \
                                 not os.path.exists(opts_settle_path)):
            hold_his = pd.DataFrame(columns=default_cols)
            hold_last = hold_his.copy()
            last_date = np.nan
        else:
            hold_his = load_csv(opts_settle_path, encoding='gbk')
            hold_his['tmp'] = hold_his.isnull().sum(axis=1)
            if hold_his['tmp'].sum() > 0:
                nanidx = hold_his[hold_his['tmp'] > 0].index[0]
                nandate = hold_his.loc[nanidx, '日期']
                hold_his = hold_his[hold_his['日期'] < \
                                                nandate].drop('tmp', axis=1)
            else:
                hold_his.drop('tmp', axis=1, inplace=True)
            if hold_his.shape[0] == 0:
                hold_last = hold_his.copy()
                last_date = np.nan
            else:
                last_date = hold_his['日期'].max()
                hold_last = hold_his[hold_his['日期'] == last_date].copy()
        return hold_his, hold_last, last_date
    
    @staticmethod
    def get_dates_settle(trade_records, last_date, trade_dates_path):
        '''
        根据交易记录历史数据，获取需要归并的所有日期列表dates
        '''
        
        dates_trade = list(trade_records['日期'].unique())

        start_date = last_date if not isnull(last_date) else min(dates_trade)
        end_date = max(today_date(), max(dates_trade))
            
        dates = get_trade_dates(start_date, end_date, trade_dates_path)
            
        if not isnull(last_date):
            dates = [x for x in dates if x > last_date]
        dates.sort()
        
        return dates, dates_trade
    
    @staticmethod
    def get_opts_settle(holds, trades, date):
        '''根据现持有合约holds和交易记录trades进行合约归并'''
        
        # 权利开仓和权利平仓汇总
        trades['持仓数量'] = trades[['开or平', '权利or义务']].apply(
            lambda x: 1 if x['开or平'] == '开' and x['权利or义务'] == '权利' \
            else (-1 if x['开or平'] == '平' and x['权利or义务'] == '权利' else 0),
            axis=1)
        trades['持仓数量'] = trades['持仓数量'] * trades['成交数量']
        trades_buy = trades.groupby(['日期', '合约代码', '权利or义务'],
                                    as_index=False).sum()
        trades_buy = trades_buy[trades_buy['权利or义务'] == '权利']
        trades_buy['总成本'] = -1 * trades_buy['交易净入']
        trades_buy = trades_buy.reindex(
                   columns=['日期', '合约代码', '权利or义务', '持仓数量', '总成本'])
    
        # 义务开仓和义务平利汇总
        trades['持仓数量'] = trades[['开or平', '权利or义务']].apply(
            lambda x: 1 if x['开or平'] == '开' and x['权利or义务'] == '义务' \
            else (-1 if x['开or平'] == '平' and x['权利or义务'] == '义务' else 0),
            axis=1)
        trades['持仓数量'] = trades['持仓数量'] * trades['成交数量']
        trades_sel = trades.groupby(['日期', '合约代码', '权利or义务'],
                                    as_index=False).sum()
        trades_sel = trades_sel[trades_sel['权利or义务'] == '义务']
        trades_sel['总成本'] = -1 * trades_sel['交易净入']
        trades_sel = trades_sel.reindex(
                   columns=['日期', '合约代码', '权利or义务', '持仓数量', '总成本'])
        
        # 与holds汇总得到最新归并持仓情况
        holds_new = pd.concat((holds, trades_buy, trades_sel), axis=0)
        holds_new['日期'] = date
        holds_new['持仓数量'] = holds_new['持仓数量'].astype(int)
        holds_new['总成本'] = holds_new['总成本'].astype(float)
        holds_new = holds_new.groupby(['日期', '合约代码', '权利or义务'],
                                                         as_index=False).sum()
        holds_new = holds_new[holds_new['持仓数量'] != 0]
        holds_new['持仓成本'] = holds_new['总成本'] / holds_new['持仓数量']
        
        holds_new = holds_new.reindex(columns=['日期', '合约代码', '权利or义务',
                                               '持仓数量', '持仓成本', '总成本'])
        
        return holds_new
    
    @staticmethod
    def get_opts_val_daily(holds, trade_records, date, opts_daily, opts_info,
                           default_cols):
        '''从opts_daily中获取合约价格并计算holds中合约持仓价值及盈亏情况'''
        
        # 从交易记录中获取最新的（截止date）合约单位
        codes = list(holds['合约代码'])
        tmp = trade_records[trade_records['合约代码'].isin(codes)]
        tmp = tmp[tmp['日期'] <= date]
        tmp = tmp[['合约代码', '合约单位', '基准单位']].copy()
        tmp.drop_duplicates(subset=['合约代码'], keep='last', inplace=True)
        holds = holds.merge(tmp, how='left', on=['合约代码'])
        
        # 从历史行情中获取价格信息计算盈亏情况
        holds = holds.merge(opts_daily[['合约代码', '日期', 'close']],
                                            how='left', on=['合约代码', '日期'])
        holds['合约价格'] = holds['close'] * holds['合约单位'] / holds['基准单位']
        holds['持仓价值'] = holds[['权利or义务', '持仓数量', '合约价格']].apply(
                            lambda x: x['持仓数量'] * x['合约价格'] * 10000 \
                                if x['权利or义务'] == '权利' \
                                else x['持仓数量'] * x['合约价格'] * -10000,
                                axis=1)
        holds['持仓盈亏%'] = holds[['总成本', '持仓价值']].apply(lambda x:
                         100 * cal_gain_pct(x['总成本'], x['持仓价值']), axis=1)
        holds = merge_df(holds, opts_info[['合约代码', '合约名称', '合约方向']],
                         same_keep='right', how='left', on=['合约代码'])
        holds = holds.reindex(columns=default_cols)
        
        return holds
    
    @staticmethod
    def update_holds(hold_his, hold_new, default_cols):
        '''更新持仓，原有持仓为hold_his，新增持仓为hold_last'''
        hold_his = hold_his.reindex(columns=default_cols)
        hold_new = hold_new.reindex(columns=default_cols)
        holds = pd.concat((hold_his, hold_new), axis=0)
        return holds
    
    def opts_settle_his(self, opts_cfg, stop_date=None):
        '''历史交易证券资产归并，opts_cfg为Opts_config对象'''
        
        # 已归并数据导入
        hold_his, hold_last, last_date = self.load_opts_settle(
                                self.opts_settle_path, self.opts_settle_cols)
        
        if self.trade_records.shape[0] == 0:
            self.hold_records = hold_his
            self.hold_last = hold_last
        else:        
            dates, dates_trade = self.get_dates_settle(self.trade_records,
                                            last_date, opts_cfg.trade_dates)
            if not isnull(stop_date):
                dates = [x for x in dates if x <= stop_date]
            
            if len(dates) == 0:
                self.hold_records = hold_his
                self.hold_last = hold_last
            else:
                for date in dates:
                    logger_show('当前日期: ' + date + ', 持仓归并...', self.logger)
                    
                    if date not in dates_trade:
                        hold_now = hold_last.copy()
                        hold_now['日期'] = date
                    else:
                        trade_records_now = self.trade_records[
                                    self.trade_records['日期'] == date].copy()
                        hold_now = self.get_opts_settle(hold_last,
                                                      trade_records_now, date)
                    if hold_now.shape[0] > 0:
                        hold_now = self.get_opts_val_daily(hold_now,
                                self.trade_records, date, opts_cfg.opts_daily,
                                opts_cfg.opts_info, self.opts_settle_cols)
                        
                    hold_last = hold_now.copy()
                    
                    hold_his = self.update_holds(hold_his, hold_now,
                                                       self.opts_settle_cols)
                    
                self.hold_records = hold_his.reset_index(drop=True)
                self.hold_last = hold_now
                
        if not isnull(self.opts_settle_path):
            logger_show('保存每日持仓归并数据...', self.logger)
            self.hold_records.to_csv(self.opts_settle_path, index=None,
                                     encoding='gbk')
            
    #%%            
    # 每日资产归并结算-----------------------------------------------------------
    def asset_settle(self, opts_cfg, stop_date=None):
        '''每日资产归并结算'''
        logger_show('每日资产归并...', self.logger)
        # 每日持仓价值
        opts_val = self.hold_records.groupby('日期', as_index=False).sum()
        opts_val.rename(columns={'持仓价值': '合约总价值'}, inplace=True)
        opts_val = opts_val.reindex(columns=['日期', '合约总价值'])
        # 每日交易资金净额
        cash_trade = self.trade_records.groupby('日期', as_index=False).sum()
        cash_trade = cash_trade.reindex(columns=['日期', '交易净入'])
        # 每日转入转出资金
        cash_flow = self.get_cash_folw_daily(self.cash_flow, opts_cfg)
        
        # 每日资产归并
        df_settle = cash_flow.merge(cash_trade, how='outer', on='日期')
        df_settle = df_settle.merge(opts_val, how='outer', on='日期')
        df_settle.fillna(0, inplace=True)
        df_settle.sort_values('日期', ascending=True, inplace=True)
        df_settle.reset_index(drop=True, inplace=True)
        
        df_settle['现金'] = 0
        for k in range(0, df_settle.shape[0]):
            if k == 0:
                df_settle.loc[k, '现金'] = df_settle.loc[k, '转入'] - \
                                          df_settle.loc[k, '转出'] + \
                                          df_settle.loc[k, '交易净入']
            else:
                df_settle.loc[k, '现金'] = df_settle.loc[k-1, '现金'] + \
                                          df_settle.loc[k, '转入'] - \
                                          df_settle.loc[k, '转出'] + \
                                          df_settle.loc[k, '交易净入']
        df_settle['资产总值'] = df_settle['现金'] + df_settle['合约总价值']
        
        if df_settle.shape[0] == 0:
            self.asset_records = df_settle.reindex(columns=['日期', '转入',
                              '转出', '交易净入', '合约总价值', '现金', '资产总值'])
        else:    
            # 既没持仓也没交易（空仓）日期也需要加入
            start_date = df_settle['日期'].min()
            end_date = df_settle['日期'].max()
            trade_dates = get_trade_dates(start_date, end_date,
                                          opts_cfg.trade_dates)
            if len(trade_dates) == 0:
                self.asset_records = df_settle.reindex(columns=['日期', '转入',
                             '转出', '交易净入', '合约总价值', '现金', '资产总值'])
            else:
                trade_dates = pd.DataFrame(trade_dates)
                trade_dates.columns = ['日期']
                df_settle = pd.merge(trade_dates, df_settle, how='left', on='日期')
                df_settle.sort_values('日期', ascending=True, inplace=True)
                df_settle['合约总价值'].fillna(method='ffill', inplace=True)
                df_settle['现金'].fillna(method='ffill', inplace=True)
                df_settle['资产总值'].fillna(method='ffill', inplace=True)
                df_settle.fillna(0, inplace=True)
                
        if not isnull(stop_date):
            df_settle = df_settle[df_settle['日期'] <= stop_date]
            
        if not isnull(self.opts_settle_path):
            logger_show('保存每日资产归并数据...', self.logger)
            df_settle.to_csv(self.asset_settle_path, index=None,
                             encoding='gbk')
        
        self.asset_records = df_settle.sort_values('日期', ascending=False)
    
    #%%
    # 盈亏统计------------------------------------------------------------------
    @staticmethod
    def get_used_daily(trade_records, hold_records):
        '''
        | 计算每日交易中实际最大占用资金量（最终目的是用于计算每日盈亏比）
        | 计算方式：
        | - 权利方: 上个交易日持仓价值+当日开仓买入实际使用资金-当日平仓卖出实际回收资金，
            逐条计算取最大值
        | - 义务方: abs(上个交易日持仓价值)+当日开仓卖出虚拟回收资金-当日平仓虚拟使用资金，
            逐条计算取最大值（这么设计更符合实际意义）                    
        '''
        
        # 权利方和义务方交易记录
        trades = trade_records.sort_values('时间', ascending=True)
        trades_right = trades[trades['权利or义务'] == '权利'].copy()
        trades_duty = trades[trades['权利or义务'] == '义务'].copy()
        # 按权利方和义务方分别计算每日持仓价值总和
        holds_val = hold_records.groupby(['日期', '权利or义务'],
                        as_index=False).sum()[['日期', '权利or义务', '持仓价值']]
        holds_right = holds_val[holds_val['权利or义务'] == '权利'].copy()
        holds_duty = holds_val[holds_val['权利or义务'] == '义务'].copy()
        
        def max_used(df):
            used_max = df['占用资金'].cumsum().max()
            date = df['日期'].unique()[0]
            return pd.DataFrame({'日期': [date], '占用资金': [used_max]})
        
        # 权利方资金占用计算
        trades_right['占用资金'] = -1 * trades_right['交易净入']            
        useds_right = trades_right.groupby('日期').apply(lambda x: max_used(x))
        useds_right.reset_index(drop=True, inplace=True)
        useds_right = pd.merge(useds_right, holds_right, how='outer', on='日期')
        useds_right.sort_values('日期', ascending=True, inplace=True)
        useds_right['占用资金'] = useds_right['占用资金'].fillna(0)
        useds_right['占用pre'] = useds_right['持仓价值'].shift(1).fillna(0)
        useds_right['占用资金'] = useds_right[['占用资金', '占用pre']].apply(
                lambda x: x['占用pre'] if x['占用资金'] < 0 else \
                          x['占用pre'] + x['占用资金'], axis=1)
            
        # 义务方资金占用计算
        trades_duty['占用资金'] = 1 * trades_duty['交易净入']
        useds_duty = trades_duty.groupby('日期').apply(lambda x: max_used(x))
        useds_duty.reset_index(drop=True, inplace=True)
        useds_duty = pd.merge(useds_duty, holds_duty, how='outer', on='日期')
        useds_duty.sort_values('日期', ascending=True, inplace=True)
        useds_duty['占用资金'] = useds_duty['占用资金'].fillna(0)
        useds_duty['占用pre'] = useds_duty['持仓价值'].shift(1).fillna(0).abs()
        useds_duty['占用资金'] = useds_duty[['占用资金', '占用pre']].apply(
                lambda x: x['占用pre'] if x['占用资金'] < 0 else \
                          x['占用pre'] + x['占用资金'], axis=1)
            
        # 每日最大占用资金合并
        used_daily = pd.concat((useds_right, useds_duty), axis=0)
        used_daily = used_daily.groupby('日期',
                                    as_index=False).sum()[['日期', '占用资金']]
        
        return used_daily
    
    @staticmethod
    def get_gain_pcts(df_settle, time_col='日期'):
        '''
        | 根据资金转入转出、资金占用和资产总值记录计算每个结算时间（time_col）的盈亏情况
        | df_settle须包含列：[time_col, '转入', '转出', '占用资金', '资产总值']
        '''
        df_settle = df_settle.sort_values(time_col, ascending=True)
        df_settle.reset_index(drop=True, inplace=True)
        df_settle['资产总值pre'] = df_settle['资产总值'].shift(1, fill_value=0)
        df_settle['盈亏'] = df_settle['资产总值'] - (df_settle['资产总值pre'] + \
                                          df_settle['转入'] - df_settle['转出'])
        df_settle['盈亏%'] = 100 * df_settle[['盈亏', '占用资金']].apply(
                  lambda x: x_div_y(x['盈亏'], x['占用资金'], v_xy0=0.0), axis=1)
        df_settle.drop('资产总值pre', axis=1, inplace=True)
        return df_settle
    
    def get_gain_stats(self):
        '''每日盈亏记录统计计算'''
        used_daily = self.get_used_daily(self.trade_records, self.hold_records)
        df_settle = pd.merge(self.asset_records, used_daily, how='left',
                             on='日期').sort_values('日期', ascending=True)
        df_settle['占用资金'] = df_settle['占用资金'].fillna(0)
        df_settle = self.get_gain_pcts(df_settle, time_col='日期')
        self.settle_records = get_gains(df_settle, gain_types=['act', 'prod',
                                                            'sum', 'fundnet'])
        self.settle_records.sort_values('日期', ascending=False, inplace=True)
        
    #%%
    # 指定最新日期进行持仓情况统计-------------------------------------------------
    def get_hold_info(self, opts_cfg, stop_date=None):
        
        self.hold_info_now = {'多头': 0.0, '空头': 0.0, '总价值': 0,
                              '多空比': np.nan}
        
        if isnull(stop_date):
            hold_last = self.hold_last
        else:
            hold_last = self.hold_records[
                                        self.hold_records['日期'] == stop_date]
            
        if hold_last.shape[0] > 0:
            self.hold_info_now['总价值'] = hold_last['持仓价值'].sum()
            
            DoKs = hold_last.groupby(
                                ['合约方向', '权利or义务']).sum()['持仓价值']
            if ('多', '权利') in DoKs.index:
                self.hold_info_now['看多权利仓（多头）'] = DoKs[('多', '权利')]
                self.hold_info_now['多头'] += DoKs[('多', '权利')]
            if ('空', '义务') in DoKs.index:
                self.hold_info_now['看空义务仓（多头）'] = DoKs[('空', '义务')]
                self.hold_info_now['多头'] -= DoKs[('空', '义务')]
            if ('空', '权利') in DoKs.index:
                self.hold_info_now['看空权利仓（空头）'] = DoKs[('空', '权利')]
                self.hold_info_now['空头'] += DoKs[('空', '权利')]
            if ('多', '义务') in DoKs.index:
                self.hold_info_now['看多义务仓（空头）'] = DoKs[('多', '义务')]
                self.hold_info_now['空头'] -= DoKs[('多', '义务')]            
                    
            if self.hold_info_now['多头'] != 0 and \
                                              self.hold_info_now['空头'] != 0:
                self.hold_info_now['多空比'] = self.hold_info_now['多头'] / \
                                              self.hold_info_now['空头']
    
#%%
if __name__ == '__main__':
    opts_cfg = OptsConfig()
    
    dirHoo = '../../RealTrading/'
    account_id = '00000000'
    account_name = 'HooOptRealTrade'
    opts_settle_path = dirHoo + 'Options/data_HooOpts/opts_settle.csv'
    # opts_settle_path = None
    trade_records_path = dirHoo + 'Options/data_HooOpts/opts_trade_record.csv'
    # trade_records_path = None
    cash_flow_path = dirHoo + 'Options/data_HooOpts/cash_flow.csv'
    asset_settle_path = dirHoo + 'Options/data_HooOpts/asset_settle.csv'
    # logger = None
    logger = simple_logger('../../log/hoo_opts.log')
    
    
    HooOptsAccount = AssetAccount(account_id=account_id,
                                  account_name=account_name,
                                  opts_settle_path=opts_settle_path,
                                  trade_records_path=trade_records_path,
                                  cash_flow_path=cash_flow_path,
                                  asset_settle_path=asset_settle_path,
                                  logger=logger)
    
    # 检查交易记录
    HooOptsAccount.check_trade_records(opts_cfg, last_date=None)    
    sure = False
    while not sure:
        isOK = input('请检查交易记录并确认继续(Enter或1)or(0)退出?\n')
        if isOK.lower() in ['1', '', 'y', 'yes']:
            sure = True
        elif isOK.lower() in ['0', 'n', 'no', 'q', 'exit']:
            raise Exception('交易记录存在异常！')
        else:
            continue
    
    # 历史持仓归并
    HooOptsAccount.opts_settle_his(opts_cfg)
    HooOptsAccount.asset_settle(opts_cfg)
        
    # 盈亏统计
    HooOptsAccount.get_gain_stats()
    n = 30
    plot_gain_act(HooOptsAccount.settle_records, n=n,
                  figsize=(8.5, 6),
                  title=f'期权实盘账户近{n}日实际盈亏%走势' if not isnull(n) else \
                        '期权实盘账户实际盈亏%走势',
                  grids=True)
    plot_gain_prod(HooOptsAccount.settle_records, n=n,
                    figsize=(8.5, 6),
                    title=f'期权实盘账户近{n}日净值走势' if not isnull(n) else \
                          '期权实盘账户净值走势',
                    grids=True)
    
    total_in = HooOptsAccount.settle_records['累计净流入'].iloc[0]
    total_now = HooOptsAccount.settle_records['资产总值'].iloc[0]
    total_gain = total_now - total_in
    print('\n账户转入总值:', total_in)
    print('账户总值:', total_now)
    print('总盈亏:', total_gain)
    print('总盈亏比: {:.4f}%'.format(total_gain / total_in * 100))

    # 最新持仓
    HooOptsAccount.get_hold_info(opts_cfg)
    print('\n')
    print('最新持仓情况：')
    for k, v in HooOptsAccount.hold_info_now.items():
        print(k+':', v)
        
        