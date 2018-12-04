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

    def wilson_score(self, pos, total, k, p_z=2.0):
        """
        威尔逊得分计算函数
        :param pos: 正例数
        :param total: 总数
        :param p_z: 正太分布的分位数，一般而言，样本数的量级越大，z的取值大
        :return: 威尔逊得分
        """
        if not isinstance(pos, float) or not isinstance(total, float) or total < 1:
            return 0.00

        # if pos > total:
        #     print('product_id: ', k)
        #     return 0.00

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

    def test(self, x_date):
        sql = '''SELECT * from stat_space.shopping_params where date='{0}';'''.format(x_date)
        result = pd.read_sql(sql, self.pgconn)
        result = result.fillna(0)
        ret = {}
        for c_click, c_show in [('list_click', 'list_show'), ('index_click', 'index_show'), ('promotion_click', 'promotion_show'),
                                ('search_click', 'search_show'), ('ad_click', 'ad_show')]:
            w_list = []
            for k, p, t in zip(result['product_id'], result[c_click], result[c_show]):
                w_list.append({'product_id': k, 'value': self.wilson_score(p, t, k), 'params': {c_click: p, c_show: t}})
            w_list.sort(key=lambda x: x.get('value'), reverse=True)
            w_list = w_list[0:50]
            [x.update({'index': w_list.index(x)+1}) for x in w_list]
            ret[c_click] = {'params': c_click + '/' + c_show, 'w_sort': w_list}

        for c_click in ['order_click', 'cart_click', 'like_click']:
            w_list = []
            for k, p, t1, t2, t3, t4, t5 in zip(result['product_id'], result[c_click], result['list_click'], result['index_click'],
                               result['promotion_click'], result['search_click'], result['ad_click']):
                t = t1 + t2 + t3 + t4 + t5
                w_list.append({'product_id': k, 'value': self.wilson_score(p, t, k), 'params': {c_click: p, c_show: t}})
            w_list.sort(key=lambda x: x.get('value'), reverse=True)
            w_list = w_list[0:50]
            [x.update({'index': w_list.index(x)+1}) for x in w_list]
            ret[c_click] = {'params': c_click + '/' + 'total_product_click', 'w_sort': w_list}

        return ret


if __name__ == '__main__':
    wv = ShoppingSort()
    q = wv.test('2018-12-02')
    print(q)
