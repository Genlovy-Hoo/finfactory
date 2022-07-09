# -*- coding: utf-8 -*-

import os
from pathlib import Path
from dramkit import StructureObject
from dramkit.iotools import load_yml


FILE_PATH = Path(os.path.realpath(__file__))


# CONFIG_PATHS = [
#     'D:/Genlovy_Hoo/HooProjects/FinFactory/config/config.yml',
#     'E:/Genlovy_Hoo/HooProjects/FinFactory/config/config.yml'
#     ]
CONFIG_PATHS = []

cfg_path = None
for fpath in CONFIG_PATHS:
    if os.path.exists(fpath):
        cfg_path = fpath
        break
if cfg_path is None:
    fpath = str(FILE_PATH.parent.parent)
    fpath = os.path.join(fpath, 'config', 'config.yml')
    if os.path.exists(fpath):
        cfg_path = fpath

cfg_yml = load_yml(cfg_path, encoding='utf-8')

cfg = StructureObject(**cfg_yml, dirt_modify=False)
