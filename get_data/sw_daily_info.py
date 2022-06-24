# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import dramkit.datetimetools as dttools
from dramkit import isnull, logger_show, load_csv
from dramkit.other.othertools import archive_data
from dramkit.fintools.utils_chn import get_trade_dates
from dramkit.fintools.utils_chn import get_recent_trade_date
from finfactory.load_his_data import find_target_dir
from finfactory.utils.utils import check_daily_data_is_new
from finfactory.utils.utils import check_date_loss


COLS_FINAL = ['code', 'name', 'date', 'open', 'high', 'low',
              'close', '成交量(万股)', '成交额(万元)', '涨跌幅(%)',
              '换手率(%)', '均价', '成交额占比(%)', 'PE', 'PB',
              '股息率(%)', '流通市值(万元)', '平均流通市值(万元)',
              'table']


def check_loss(data, trade_dates, logger=None):
    '''检查缺失'''
    loss_dates = check_date_loss(data,
                                 trade_dates_df_path=trade_dates)
    if len(loss_dates) > 0:
        logger_show('申万一级行业日数据有缺失日期：'+','.join(loss_dates),
                    logger, 'warn')
    return loss_dates


def _read_aspx(fpath):
    '''读取申万exce格式数据'''
    
    with open(fpath, encoding='utf-8') as f:
        data = f.readlines()[0]
    data = BeautifulSoup(data, 'lxml')
    cols = data.find_all('th')
    cols = [x.get_text() for x in cols]
    
    df = data.find_all('tr')[1:]
    df = [[x.getText() for x in td.findAll('td')] for td in df]
    df = pd.DataFrame(df, columns=cols)
    df['发布日期'] = df['发布日期'].apply(lambda x: x.split(' ')[0])
    df['发布日期'] = df['发布日期'].apply(lambda x: '-'.join([y.zfill(2) for y in x.split('/')]))
    
    return df


def get_codes():
    '''获取（当前最新的）申万一级行业指数代码'''
    url = 'http://www.swsindex.com/idx0200.aspx?columnid=8838&type=Day'
    html = requests.get(url)
    bsObj = BeautifulSoup(html.content, 'lxml')
    codes = bsObj.find(attrs={'id': 'dpindextype', 'name': 'dpindextype'})
    codes = codes.find_all('option')
    codes = [x['value'] for x in codes]
    codes = [x for x in codes if x != '']
    return codes


def _save_ori(df, date, save_ori,
              ori_save_path, root_dir, logger):
    if not save_ori or isnull(df):
        return
    if isnull(ori_save_path):
        save_dir = find_target_dir('sw/daily_ori/',
                   root_dir=root_dir, make=True, logger=logger)
        save_path = save_dir + date + '.csv'
        df.to_csv(save_path, index=None, encoding='gbk')


def get_daily_info(date, save_ori=False,
                   ori_save_path=None, root_dir=None,
                   logger=None):
    '''
    | http://www.swsindex.com/idx0200.aspx?columnid=8838&type=Day
    | http://www.swsindex.com/idx0130.aspx?columnid=8838
    '''
    
    date = dttools.date_reformat(date, '-')
    
    url = 'http://www.swsindex.com/handler.aspx'
    headers = {
        'Host': 'www.swsindex.com',
        'Origin': 'http://www.swsindex.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
    }
    req_data = {
        'key': 'id',
        'orderby': 'swindexcode asc,BargainDate_1',
        'fieldlist': 'SwIndexCode,SwIndexName,BargainDate,CloseIndex,BargainAmount,Markup,TurnoverRate,PE,PB,MeanPrice,BargainSumRate,NegotiablesShareSum,NegotiablesShareSum2,DP,OpenIndex,MaxIndex,MinIndex,BargainSum',
        'pagecount': '1000000000'
    }    
    
    def _get_data(tablename):
        req_data['tablename'] = tablename
        req_data['where'] = "BargainDate>='{}' and BargainDate<='{}'".format(date, date)
        if tablename == 'V_Report':
            req_data['where'] += " and type='Day'"
        end_, df, p = False, [], 1
        while not end_:
            logger_show('{}, {}, page {} ...'.format(date, tablename, p),
                        logger)
            req_data['p'] = '{}'.format(p)
            r = requests.post(url, headers=headers, data=req_data)
            bsObj = BeautifulSoup(r.content, 'lxml')
            data = eval(bsObj.find('p').get_text())['root']
            if len(data) > 0:
                df += data
                p += 1
            else:
                end_ = True
        df = pd.DataFrame(df)
        df['table'] = tablename
        return df
    
    df1 = _get_data('V_Report')
    df2 = _get_data('swindexhistory')
    
    df = pd.concat((df1, df2), axis=0)
    df.rename(columns={'SwIndexCode': 'code',
                       'SwIndexName': 'name',
                       'BargainDate': 'date',
                       'OpenIndex': 'open',
                       'MaxIndex': 'high',
                       'MinIndex': 'low',
                       'CloseIndex': 'close',
                       'BargainAmount': '成交量(万股)',
                       'Markup': '涨跌幅(%)',
                       'TurnoverRate': '换手率(%)',
                       'MeanPrice': '均价',
                       'BargainSumRate': '成交额占比(%)',
                       'NegotiablesShareSum': '流通市值(万元)',
                       'NegotiablesShareSum2': '平均流通市值(万元)',
                       'DP': '股息率(%)',                       
                       'BargainSum': '成交额(万元)'},
              inplace=True)
    df = df[COLS_FINAL]
    
    _save_ori(df, date, save_ori,
              ori_save_path, root_dir, logger)
    
    return df


# def get_1stinds_daily(codes=None, start_date='2019-01-01',
#                       end_date=None, save_dir=None,
#                       save_ori=False, logger=None):
#     '''
#     更新申万一级行业指数日数据
#     '''
    
#     if isnull(codes):
#         codes = get_codes()
    
#     if isnull(start_date):
#         start_date = '1999-12-30'
#     if isnull(end_date):
#         end_date = get_recent_trade_date(dirt='pre')
#     start_date = dttools.date_reformat(start_date, '-')
#     end_date = dttools.date_reformat(end_date, '-')
    
#     codes_str = [f'%27{x}%27' for x in codes]
#     codes_str = ','.join(codes_str)
#     url = f'http://www.swsindex.com/excel.aspx?ctable=V_Report&where=%20%20swindexcode%20in%20({codes_str})%20and%20%20BargainDate%3E=%27{start_date}%27%20and%20%20BargainDate%3C=%27{end_date}%27%20and%20type=%27Day%27'
    
#     logger_show(f'{start_date}——>{end_date}下载中...', logger, 'info')
#     urllib.request.urlretrieve(url, save_path)
    
#     df = read_aspx(save_path)
    
#     os.remove(save_path)
    
#     return df


# df1 = archive_data(df, df_ori,
#                    sort_cols=['发布日期', '指数代码'],
#                    del_dup_cols=['发布日期', '指数代码'],
#                    sort_first=False,
#                    csv_path='sw_1stinds_daily.csv',
#                    csv_index=None,
#                    csv_encoding='gbk')
                   


# def update_index_pe(eniu_code, df_exist=None,
#                     fpath=None, logger=None):
#     '''
#     | 从亿牛网爬取更新指数PE估值日数据
#     | eniu_code格式如：sz399300、sh000016
#     | https://eniu.com/
#     '''
    
#     if isnull(df_exist):
#         if not isnull(fpath) and os.path.exists(fpath):
#             df_exist = load_csv(fpath)
#     if not isnull(df_exist):
#         df_exist = df_exist[COLS_FINAL].copy()
        
#     df = get_1stinds_daily(eniu_code)    
    
#     df_all = archive_data(df, df_exist,
#                           sort_cols=['date'],
#                           del_dup_cols=['date'],
#                           sort_first=False,
#                           csv_path=fpath,
#                           csv_index=None)
    
#     is_new, info = check_daily_data_is_new(df_all)
#     if is_new:
#         logger_show('{}指数PE日数据更新完成！'.format(eniu_code),
#                     logger, 'info')
#     else:
#         logger_show('{}指数PE日数据更新未完成！数据最后日期：{}'.format(eniu_code, info[1]),
#                     logger, 'warn')
        
#     return df_all


# def update_index_pe_check(eniu_code, 
#                           save_path=None,
#                           root_dir=None,
#                           trade_dates=None,
#                           logger=None):
#     '''从亿牛网爬取更新指数PE估值日数据'''
    
#     def _get_save_path(save_path):
#         '''获取指数PE估值日数据存放路径'''
#         if isnull(save_path):
#             from finfactory.load_his_data import find_target_dir
#             save_dir = find_target_dir('index/eniu/',
#                        root_dir=root_dir, make=True, logger=logger)
#             save_path = save_dir + '{}_pe_daily.csv'.format(eniu_code)
#         return save_path
    
#     save_path = _get_save_path(save_path)
#     df_all = update_index_pe(eniu_code,
#                              df_exist=None,
#                              fpath=save_path,
#                              logger=logger)
    
#     loss_dates = check_loss(df_all, eniu_code, trade_dates, logger)
    
#     df_all.sort_values('date', ascending=False, inplace=True)
#     return df_all, loss_dates


if __name__ == '__main__':
    import sys
    import time
    from dramkit import close_log_file
    from dramkit.gentools import try_repeat_run
    from finfactory.load_his_data import load_trade_dates_tushare
    from finfactory.config import cfg
    from finfactory.utils.utils import gen_py_logger
    strt_tm = time.time()
    
    
    logger = gen_py_logger(sys.argv[0])
    
    
    # @try_repeat_run(cfg.try_get_eastmoney, logger=logger,
    #                 sleep_seconds=cfg.try_get_eastmoney_sleep)
    # def try_update_index_pe_check(*args, **kwargs):
    #     return update_index_pe_check(*args, **kwargs)
    
    trade_dates = load_trade_dates_tushare('SSE')
    # trade_dates = None
    
    # df, loss = try_update_index_pe_check(
    #                                     save_path=None,
    #                                     root_dir=None,
    #                                     trade_dates=trade_dates,
    #                                     logger=logger)
    
    fdir = find_target_dir('sw/daily_ori/', make=True)
    dates = get_trade_dates('1999-12-30',
                            trade_dates_df_path=trade_dates)
    for date in dates:
        fpath = fdir + '{}.csv'.format(date)
        if not os.path.exists(fpath):
            try:
                df = get_daily_info(date, logger)
                # df.to_csv(fpath, encoding='gbk', index=None)
                logger_show('pausing ...', logger)
                time.sleep(10)
            except:
                logger_show('{}失败！'.format(date), logger, 'warn')
                pass
    
    
    close_log_file(logger)
    
    
    close_log_file(logger)
    
    
    print(f'used time: {round(time.time()-strt_tm, 6)}s.')
    
    