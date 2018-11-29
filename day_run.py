#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/29 10:37
# @Author  : zhangpeng
# @File    : day_run.py
# 说明     :  跑每天执行的数据

import datetime
from download_data import MatomoApi
from gen_statdata import StatData

if __name__ == '__main__':
    mapi = MatomoApi()
    # mapi.n_run(9)
    mapi.run((datetime.datetime.today() - datetime.timedelta(days=1)).date())
    mapi.save_product()  # 获取商城整站product数据

    sd = StatData()
    sd.gen_sql_stat(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))
