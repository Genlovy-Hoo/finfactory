# -*- coding: utf-8 -*-

import os
from pathlib import Path
from dramkit import isnull
from dramkit import StructureObject
from dramkit.iotools import load_yml


FILE_PATH = Path(os.path.realpath(__file__))


# 默认配置文件路径查找顺序，根据需要修改
config_paths = [
    'D:/FinFactory/config/config.yml',
    'E:/FinFactory/config/config.yml',
    'F:/FinFactory/config/config.yml',
    'G:/FinFactory/config/config.yml',
    'D:/Genlovy_Hoo/HooProjects/FinFactory/config/config.yml',
    'E:/Genlovy_Hoo/HooProjects/FinFactory/config/config.yml'
    ]

fpath = str(FILE_PATH.parent.parent)
fpath = os.path.join(fpath, 'config', 'config.yml')
config_paths.append(fpath)

cfg_path = None
for fpath in config_paths:
    if os.path.exists(fpath):
        cfg_path = fpath
        break

if not isnull(cfg_path):
    cfg_yml = load_yml(cfg_path, encoding='utf-8')
    cfg = StructureObject(**cfg_yml)
else:    
    cfg = StructureObject()

# 根据需要修改下面的参数
cfg.set_from_dict(
    {
    # 默认数据存档根目录
    'archive_roots': [
        'D:/FinFactory/data/archives/',
        'E:/FinFactory/data/archives/',
        'F:/FinFactory/data/archives/',
        'G:/FinFactory/data/archives/',
        'D:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/',
        'E:/Genlovy_Hoo/HooProjects/FinFactory/data/archives/'
        ],
    
    # 默认日志目录
    'log_dirs': [
        'D:/FinFactory/log/',
        'E:/FinFactory/log/',
        'F:/FinFactory/log/',
        'G:/FinFactory/log/',
        'D:/Genlovy_Hoo/HooProjects/FinFactory/log/',
        'E:/Genlovy_Hoo/HooProjects/FinFactory/log/'
        ],
     
    # 运行Python脚本时是否不保存日志文件
    'no_py_log': True,
    
    # 用ccxt取数据尝试次数和时间间隔（秒）
    'try_get_ccxt': 1,
    'try_get_ccxt_sleep': 10,
    
    # tushare取数尝试次数和间隔时间（秒）
    'try_get_tushare': 1,
    'try_get_tushare_sleep': 10,
    
    # 从财政部网站下载国债和地方债收益率尝试次数和时间间隔（秒）
    'try_get_chn_bond_rates': 1,
    'try_get_chn_bond_rates_sleep': 10,
    
    # 从中金所下载期货龙虎榜数据尝试次数和时间间隔（秒）
    'try_get_cffex': 1,
    'try_get_cffex_sleep': 10,
    
    # 爬取东财数据尝试次数和时间间隔（秒）
    'try_get_eastmoney': 1,
    'try_get_eastmoney_sleep': 10,
    
    # 爬取和讯网数据尝试次数和时间间隔（秒）
    'try_get_hexun': 1,
    'try_get_hexun_sleep': 10,
    
    # 爬取亿牛网数据尝试次数和时间间隔（秒）
    'try_get_eniu': 1,
    'try_get_eniu_sleep': 10,
    
    # 爬取申万数据尝试次数和时间间隔（秒）
    'try_get_sw': 1,
    'try_get_sw_sleep': 10,
    
    # 爬取易方达数据尝试次数和时间间隔（秒）
    'try_get_fundex': 3,
    'try_get_fundex_sleep': 15,
    
    # tushare接口每分钟限制调用次数
    'ts_1min_daily': 400, # 股票日线行情接口
    'ts_1min_daily_basic': 400, # 45 # 股票日线基本数据接口 ***
    'ts_1min_stk_factor': 99, # 股票技术因子接口 ***
    'ts_1min_block_trade': 20, # 大宗交易接口
    'ts_1min_us_tycr': 8, # 美国债收益率接口
    'ts_1min_us_trycr': 8, # 美国债实际收益率接口
    'ts_1min_cctv_news': 30, # 新闻联播文本数接口
    'ts_1min_fut_daily': 15, # 期货日线行情接口
    'ts_1min_fut_mapping': 100, # 主力/连续合约映射接口
    'ts_1min_shibor': 8, # shibo利率接口
    'ts_1min_shibor_lpr': 8, # LPR利率接口
    'ts_1min_opt_daily': 10, # 4 # 期权日线接口 ***
    'ts_1min_opt_basic': 10, # 1 # 期权合约信息接口 ***
    'ts_1min_top10holders': 300, # 8 # 前十大股东接口 ***
    'ts_1min_top10holders_free': 300, # 8 # 前十大流通股东接口 ***
    'ts_1min_zjc': 250, # 8 # 股东增减持接口 ***
    }
)
