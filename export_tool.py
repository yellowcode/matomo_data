#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/30 9:49
# @Author  : zhangpeng
# @File    : export_tool.py
# 说明     : 一些导出函数


import pandas as pd
from pgsql_pool import PgsqlConn


if __name__ == '__main__':
    pgdb = PgsqlConn()
    pgconn = pgdb.pgsql_conn()
    sql = "SELECT * FROM stat_space.shopping_params where date='2018-11-29'"
    df = pd.read_sql(sql, pgconn)
    df.to_excel('/project/pandas_output.xls')

