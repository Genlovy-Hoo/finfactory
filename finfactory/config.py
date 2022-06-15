# -*- coding: utf-8 -*-

import os
from dramkit.iotools import load_yml
from dramkit import StructureObject


CONFIG_PATHS = [
    'D:/Genlovy_Hoo/HooProjects/FinFactory/config/config.yml',
    'E:/Genlovy_Hoo/HooProjects/FinFactory/config/config.yml'
    ]

cfg_path = None
for fpath in CONFIG_PATHS:
    if os.path.exists(fpath):
        cfg_path = fpath
        break

cfg_yml = load_yml(cfg_path, encoding='utf-8')

cfg = StructureObject(**cfg_yml)