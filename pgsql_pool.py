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
        # self.__engine_space = create_engine('postgresql://penpen:peng1479@192.144.129.168:54320/matomo', pool_recycle=300)
        self.__engine_space = create_engine('postgresql://postgres:syt2018@localhost:5432/postgres')

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
#     action = '''SELECT distinct aa.url FROM public.action aa WHERE aa.url ~ '-p-' and aa.url ~ 'm.dws';'''
#     ss = db_pool.execute(action)
#     a = [x[0] for x in ss.fetchall()]
#
#     event = '''SELECT distinct url, eventaction,eventname FROM public."event" ee WHERE ee.url ~ '-p-' and ee.url ~ 'm.dws';'''
#     ss = db_pool.execute(event)
#     e = [x[0] for x in ss.fetchall()]
#
#     for x in e:
#         if x not in a:
#             print(x)
