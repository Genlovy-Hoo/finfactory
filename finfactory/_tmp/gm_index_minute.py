# -*- coding: utf-8 -*-

from finfactory.utils.utils import get_gm_api

gm_api = get_gm_api()

# code = 'SHSE.688008'
# code = 'SHSE.000300'
code = 'CFFEX.IF'
frequency = '60s'
frequency = '1d'
frequency = '900s'
start_time = '2022-06-01 00:00:00'
end_time = '2022-06-12 00:00:00'
df1 = gm_api.history(code, frequency, start_time=start_time,
                     end_time=end_time, fields=None,
                     skip_suspended=False,
                     fill_missing='NaN',
                     adjust=gm_api.ADJUST_NONE,
                     adjust_end_time='', df=True)