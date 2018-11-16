#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/14 14:20
# @Author  : zhangpeng
# @Site    : pgsql_pool.py
# @File    :  创建pgsql连接模块


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# from sqlalchemy.types import

class PgsqlConn(object):

    def __init__(self):
        self.__engine_space = create_engine('postgresql://penpen:peng1479@192.144.129.168:54320/matomo', pool_recycle=300)

    def pgsql_conn(self):
        """
        :return: 数据仓库 sqlalchemy模板create_engine
        """
        return self.__engine_space

    def sqlalchemy_conn(self):
        """
        :return:  :return: 数据仓库链接对象
        """
        S = sessionmaker(bind=self.__engine_space)
        session = S()
        return session


# if __name__ == '__main__':
#     pgdb = PgsqlConn()
#     db_pool = pgdb.pgsql_conn()
#     sql = '''select aa.COLUMN_NAME from information_schema.columns aa where table_schema='public' and "table_name"='visit_details';'''
#     ss = db_pool.execute(sql)
#     print([x[0] for x in ss.fetchall()])

