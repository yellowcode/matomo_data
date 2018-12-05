#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/16 14:59
# @Author  : zhangpeng
# @File    : model.py
# 说明     :  计算模型


import math
import numpy as np
import pandas as pd
from pgsql_pool import PgsqlConn


class ShoppingSort(object):

    def __init__(self):
        pgdb = PgsqlConn()
        self.pgconn = pgdb.pgsql_conn()

    def wilson_score(self, pos, total, p_z=2.0):
        """
        威尔逊得分计算函数
        :param pos: 正例数
        :param total: 总数
        :param p_z: 正太分布的分位数，一般而言，样本数的量级越大，z的取值大
        :return: 威尔逊得分
        """
        if not isinstance(pos, float) or not isinstance(total, float) or total < 1:
            return 0.00

        if pos > total:
            pos = total = (pos + total)/2

        try:
            pos_rat = pos * 1.0 / total * 1.0  # 正例比率
            a = pos_rat + (np.square(p_z) / (2.0 * total))
            b = (p_z / (2.0 * total)) * math.sqrt(4.0 * total * (1.0 - pos_rat) * pos_rat + np.square(p_z))
            c = (1.0 + np.square(p_z) / total)
            ret = round((a - b) / c, 6)
        except Exception as e:
            print(k, '---', pos, '---', total, ' e: ', e)
            ret = round(0.00, 6)

        if 'nan' in str(ret):
            return np.nan_to_num(ret)

        return ret

    def calculate_sort(self, x_date, filter=-1):
        sql = '''SELECT * from stat_space.shopping_params where date='{0}';'''.format(x_date)
        result = pd.read_sql(sql, self.pgconn)
        result = result.fillna(0)
        ret = {}
        for c_click, c_show in [('list_click', 'list_show'), ('index_click', 'index_show'), ('promotion_click', 'promotion_show'),
                                ('search_click', 'search_show'), ('ad_click', 'ad_show'), ('order_click', 'total_detail_click'),
                                ('cart_click', 'total_detail_click'), ('like_click', 'total_detail_click')]:
            w_list = []
            for k, p, t in zip(result['product_id'], result[c_click], result[c_show]):
                w_list.append({'product_id': k, 'value': self.wilson_score(p, t, k), 'params': {c_click: p, c_show: t}})
            w_list.sort(key=lambda x: x.get('value'), reverse=True)
            if filter > 0:
                w_list = w_list[0:50]
            [x.update({'index': w_list.index(x)+1}) for x in w_list]
            ret[c_click] = {'params': c_click + '/' + c_show, 'w_sort': w_list}

        return ret

    def test_calculate_sort(self, x_date):
        sql = '''SELECT * from stat_space.shopping_params_copy1 where date='{0}';'''.format(x_date)
        df = pd.read_sql(sql, self.pgconn)
        df.fillna(0, inplace=True)
        for c_click, c_show in [('list_click', 'list_show'), ('index_click', 'index_show'), ('promotion_click', 'promotion_show'),
                                ('search_click', 'search_show'), ('ad_click', 'ad_show'), ('order_click', 'total_detail_click'),
                                ('cart_click', 'total_detail_click'), ('like_click', 'total_detail_click')]:
            df['w_' + c_click] = [self.wilson_score(p, t) for p, t in zip(df[c_click], df[c_show])]
            # df.sort_values(by='w_' + c_click, ascending=False, inplace=True)    # 按一列排序
        return df

    def weight_sort(self, data):
        ret = {}
        for col in data:
            print(col)
            if 'w_' not in col:
                continue
            avg = round(data[col].mean(), 7)
            sdr = round(data[col].std(), 7)
            print(avg)
            print(sdr)
            q = round(sdr / avg, 7)
            ret[col] = {'avg': avg, 'sdr': sdr, 'q': q}

        # 权重可在此处调整

        return ret

    def cul_run(self, x_date):
        re_data = self.test_calculate_sort(x_date)
        re_w = self.weight_sort(re_data)
        print(re_w)


if __name__ == '__main__':
    wv = ShoppingSort()
    wv.cul_run('2018-12-04')
