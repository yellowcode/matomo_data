#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/29 10:37
# @Author  : zhangpeng
# @File    : day_run.py
# 说明     :  跑每天执行的数据

import datetime
from download_data import MatomoApi
from gen_statdata import StatData
from model import ShoppingSort
from ab_test import AbTest


if __name__ == '__main__':
    # 基础数据
    mapi = MatomoApi()
    # mapi.n_run(9)
    mapi.run((datetime.datetime.today() - datetime.timedelta(days=1)).date())
    mapi.save_product()  # 获取商城整站product_id数据
    mapi.product_data(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))     # event补齐product字段
    mapi.save_order_product(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))   # 获取订单数据

    # 统计数据
    sd = StatData()
    # for x in range(1, 8):
    #     sd.gen_sql_stat(str((datetime.datetime.today() - datetime.timedelta(days=x)).date()))
    sd.gen_sql_stat(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))

    # model计算
    wv = ShoppingSort()
    wv.cul_run(str((datetime.datetime.today() - datetime.timedelta(days=int(1))).date()))
    # wv.cul_run(str((datetime.datetime.today() - datetime.timedelta(days=int(1))).date()))
    # for x in '7654321':
    #     wv.cul_run(str((datetime.datetime.today() - datetime.timedelta(days=int(x))).date()))
    # wv.save_data(str((datetime.datetime.today() - datetime.timedelta(days=int(x))).date()))
    # wv.sort_run(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))
    # wv.write_excel()

    # a/b测试统计代码
    abtest = AbTest()
    abtest.shopping_change(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))
