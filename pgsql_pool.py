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
        pass

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

    def mysql_pgsql_conn(self):
        """
        :return: 数据仓库 sqlalchemy模板create_engine
        """
        return self.__mysql_space

    def mysql_sqlalchemy_conn(self):
        """
        :return:  :return: 数据仓库链接对象
        """
        S = sessionmaker(bind=self.__mysql_space)
        session = S()
        return session

    def sort_mysql_conn(self):
        """
        :return: 数据仓库 sqlalchemy模板create_engine
        """
        return self.__mysql_sort

    def sort_sqlalchemy_mysql_conn(self):
        """
        :return:  :return: 数据仓库链接对象
        """
        S = sessionmaker(bind=self.__mysql_sort)
        session = S()
        return session


# if __name__ == '__main__':
#     pgdb = PgsqlConn()
#     db_pool = pgdb.mysql_pgsql_conn()
#     sql = '''select id,rating,sort from cc_products limit 100;'''
#     ss = db_pool.execute(sql)
#     q = [x[0] for x in ss.fetchall()]
#     print(q)

