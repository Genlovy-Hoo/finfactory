# -*- coding: utf-8 -*-

from ._pkg_info import pkg_info

try:
    from dramkit import simple_logger, logger_show
except:
    print('Please install `dramkit` via `pip install dramkit --upgrade`')


suscess_str = '''安装成功, 版本: %s.
      查看更多信息请使用命令: `%s.pkg_info`,
      修改配置请前往脚本文件: `%s.config.py`
''' % (pkg_info['__version__'],
       pkg_info['__pkgname__'],
       pkg_info['__pkgname__'])


def install_check():
    '''
    检查是否成功安装finfactory

    若成功安装，会打印版本号和相关提示信息
    '''
    
    logger = simple_logger()
    
    try:
        from finfactory import config
        from finfactory.load_his_data import find_target_dir
        logger_show(suscess_str , logger, 'info')
    except:
        print('未成功安装finfactory, 请检查！')