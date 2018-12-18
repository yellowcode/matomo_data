#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/16 14:59
# @Author  : zhangpeng
# @File    : model.py
# 说明     :  计算模型


# import os
import datetime
import math
import numpy as np
import pandas as pd
from pgsql_pool import PgsqlConn


class ShoppingSort(object):

    def __init__(self):
        pgdb = PgsqlConn()
        self.pgconn = pgdb.pgsql_conn()
        self.sqlalchemy_conn = pgdb.sqlalchemy_conn()
        self.mysql_conn = pgdb.mysql_sqlalchemy_conn()
        self.sort_mysql = pgdb.sort_mysql_conn()

        self.site = 'm.dwstyle.com'
        self.writer = pd.ExcelWriter('/root/project/data_files/stat_%s.xls' % str(datetime.datetime.today().date()))
        self.excel_field = ["product_id", "order_click", "cart_click", "like_click", "total_detail_click",
                            "index_click", "index_show", "promotion_click", "promotion_show", "ad_click",
                            "ad_show", "list_click", "list_show", "search_click", "search_show", "detail_click",
                            "detail_show", "w_order_click", "w_cart_click", "w_like_click", "w_index_click",
                            "w_promotion_click", "w_ad_click", "w_list_click", "w_search_click",
                            "value", "sort", "date", "order", "pay"]

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
            total = pos + total

        try:
            pos_rat = pos * 1.0 / total * 1.0  # 正例比率
            a = pos_rat + (np.square(p_z) / (2.0 * total))
            b = (p_z / (2.0 * total)) * math.sqrt(4.0 * total * (1.0 - pos_rat) * pos_rat + np.square(p_z))
            c = (1.0 + np.square(p_z) / total)
            ret = round((a - b) / c, 6)
        except Exception as e:
            print(pos, '---', total, ' e: ', e)
            ret = round(0.00, 6)

        if 'nan' in str(ret):
            return np.nan_to_num(ret)

        return ret

    def calculate_sort(self, x_date):
        sql = '''SELECT * from stat_space.shopping_params where date='{0}';'''.format(x_date)
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
            if 'w_' not in col:
                continue
            avg = round(data[col].mean(), 7)
            sdr = round(data[col].std(), 7)
            q = round(sdr / avg, 7)
            ret[col] = {'avg': avg, 'sdr': sdr, 'q': q}

        q_sum = sum([v.get('q') for k, v in ret.items()])
        for k, v in ret.items():
            ret[k] = v.get('q') / q_sum

        # 权重可在此处调整
        ret['w_order_click'] = ret['w_order_click'] + 0.4
        ret['w_cart_click'] = ret['w_cart_click'] + 0.3
        ret['w_like_click'] = ret['w_like_click'] + 0.2

        ret = dict([(x, ret.get(x)/sum(ret.values())) for x in list(ret.keys())])

        ret = dict(zip(["w_order_click", "w_cart_click", "w_like_click", "w_index_click", "w_promotion_click", "w_ad_click", "w_list_click", "w_search_click"],
                       [0.1157, 0.1517, 0.0557, 0.054, 0.0882, 0.2757, 0.2103, 0.0487]))

        return ret

    def stat_order(self, x_date, pls, tp=1):
        """
        获取订单信息
        :param x_date: 日期
        :param pls: porduct_id 列表
        :param tp:  订单状态
        :return:
        """
        if isinstance(x_date, str):
            if tp == 2:
                sql = ('''SELECT product_id, sum(qty) as num FROM product_order WHERE date='{0}' and order_status in {1}
                GROUP BY product_id''').format(x_date, '(2,4,5,8,10,11,12)')
            else:
                sql = ('''SELECT product_id, sum(qty) as num FROM product_order WHERE date='{0}' 
                                GROUP BY product_id''').format(x_date, tp)
            df = pd.read_sql(sql, self.pgconn)
            result = dict([(str(x[0]), x[1]) for x in zip(df['product_id'], df['num'])])
            return [result.get(str(x)) if str(x) in result else 0 for x in pls]
        elif isinstance(x_date, tuple):
            if len(x_date) == 1:
                x_date = str(x_date).replace(',', '')
            else:
                x_date = str(x_date)
            if tp == 2:
                sql = ('''SELECT product_id, sum(qty) as num FROM product_order WHERE date in {0} and order_status in {1}
                GROUP BY product_id''').format(x_date, '(2,4,5,8,10,11,12)')
            else:
                sql = ('''SELECT product_id, sum(qty) as num FROM product_order WHERE date in {0} 
                                GROUP BY product_id''').format(x_date, tp)
            df = pd.read_sql(sql, self.pgconn)
            result = dict([(str(x[0]), x[1]) for x in zip(df['product_id'], df['num'])])
            return [result.get(str(x)) if str(x) in result else 0 for x in pls]
        else:
            return [0]*len(pls)

    def cul_run(self, x_date):
        re_data = self.calculate_sort(x_date)
        re_w = self.weight_sort(re_data)
        cols = [x for x in list(re_data.columns) if 'w_' in x]
        re_data['value'] = re_data[cols[0]]*re_w.get(cols[0]) + \
                                 re_data[cols[1]]*re_w.get(cols[1]) + \
                                 re_data[cols[2]]*re_w.get(cols[2]) + \
                                 re_data[cols[3]]*re_w.get(cols[3]) + \
                                 re_data[cols[4]]*re_w.get(cols[4]) + \
                                 re_data[cols[5]]*re_w.get(cols[5]) + \
                                 re_data[cols[6]]*re_w.get(cols[6]) + \
                                 re_data[cols[7]]*re_w.get(cols[7])

        re_data.sort_values(by='value', ascending=False, inplace=True)  # 按一列排序
        re_data['sort'] = [x for x in range(1, len(re_data.index) + 1)]
        # 增加数据生成excel文档
        re_data['order'] = self.stat_order(str(x_date), list(re_data['product_id']), tp=1)
        re_data['pay'] = self.stat_order(str(x_date), list(re_data['product_id']), tp=2)
        re_data.to_excel(self.writer, sheet_name=str(x_date), columns=self.excel_field)
        self.writer.save()

        mysql_redata = re_data.drop('value', axis=1)
        mysql_redata.drop('sort', axis=1, inplace=True)
        mysql_redata.to_sql('statday', self.sort_mysql, if_exists='append', index=False)


        df = re_data[['product_id', 'value', 'sort']]
        d_word = tuple(re_data['product_id'])
        sql = ('''select product_id from stat_space.product where product_id not in {0} 
        ORDER BY create_time DESC;''').format(str(d_word))
        pdata = pd.read_sql(sql, self.pgconn)
        pdata['sort'] = [x for x in range(len(d_word) + 1, len(pdata.index) + len(d_word) + 1)]
        pdata['value'] = float(0.00)
        df.append(pdata, ignore_index=False)
        df.reset_index(drop='index', inplace=True)

        return df

    def write_data(self, data):
        sql = '''update cc_products set sort={1} where id='{0}';'''
        n = 0
        for p, v in zip(data['product_id'], data['sort']):
            n = n + 1
            e_sql = sql.format(p, v)
            self.mysql_conn.execute(e_sql)

            if n % 500 == 0:
                print('mysql sort update: ', n)
                self.mysql_conn.commit()

        self.mysql_conn.commit()
        # os.system('cd /home/dwstyle/wwwroot/public;php artisan    ccshop:reflush-es    --force')

    def weigth_avg(self, data):
        if sum(data) == 0:
            return 0.00

        ws = [0.02403, 0.1351, 0.20645, 0.27544, 0.32568, 0.3784, 0.408413]
        ws = [x/sum(ws) for x in ws]
        data_ws = [(k, v) for k, v in zip(data, ws) if k]
        v = [x for x, y in data_ws]
        w = [y for x, y in data_ws]
        if w:
            return round(np.average(v, weights=w), 7)
        else:
            return 0.00

    def save_data(self, x_date):
        df = self.cul_run(x_date)
        print(df.head())
        # sql = '''select product_id from stat_space.sort_result;'''
        # result = self.pgconn.execute(sql)
        # product_list = set([int(x[0]) for x in result.fetchall() if x])
        # df_list = set([int(x) for x in df['product_id'] if x])
        # new_p = df_list.difference(product_list)
        # if new_p:
        #     ndp = pd.DataFrame(data=list(new_p), columns=['product_id'], dtype='int')
        #     ndp.to_sql('sort_result', self.pgconn, schema='stat_space', if_exists='append', index=False)
        #
        # week_map = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        # # yesterday = datetime.date.today() - datetime.timedelta(days=1)
        # day = datetime.datetime.strptime(x_date, '%Y-%m-%d').weekday()
        # field = week_map[day]
        # df[field] = round(df['value'], 7)
        # df.fillna(0.00)
        # sql = '''update stat_space.sort_result set {2}={1} where product_id={0};'''
        # n = 0
        # for p, v in zip(df['product_id'], df['value']):
        #     n = n + 1
        #     e_sql = sql.format(p, v, field)
        #     self.pgconn.execute(e_sql)
        # print('today is ', field)

    def sort_run(self, x_date):
        sql = ('''select product_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday 
        from stat_space.sort_result;''')
        df = pd.read_sql(sql, self.pgconn)
        new_data = [{'product_id': row['product_id'],
                     'value': self.weigth_avg([row['monday'], row['tuesday'], row['wednesday'], row['thursday'],
                                               row['friday'], row['saturday'], row['sunday']])}
                    for index, row in df.iterrows()]
        new_data.sort(key=lambda x: x.get('value'), reverse=True)
        [new_data[i].update({'sort': i+1, 'date': x_date}) for i in range(len(new_data))]
        [new_data[i].update({'value': 0.00}) for i in range(len(new_data)) if not new_data[i].get('value')]
        sql = '''update stat_space.sort_result set value={1},sort={2},date='{3}' where product_id={0};'''
        n = 0
        for dt in new_data:
            n = n + 1
            e_sql = sql.format(dt.get('product_id'), dt.get('value'), dt.get('sort'), dt.get('date'))
            self.pgconn.execute(e_sql)

    def get_dates(self, n):
        """
        从今天起的几天
        :param n: 天数
        :return:
        """
        if n < 1:
            return

        days = []
        for x in range(0, n):
            days.append(str((datetime.datetime.today() - datetime.timedelta(days=x+1)).date()))

        return tuple(days)

    def write_excel(self):
        sql = ('''select a.*,b.category,b.subcategory from stat_space.sort_result a, stat_space.product b 
        where a.product_id=b.product_id;''')
        df = pd.read_sql(sql, self.pgconn)
        df['value'].fillna(0.00)
        df['sort'].fillna(99999)
        df['order1'] = self.stat_order(self.get_dates(1), list(df['product_id']), tp=1)
        df['order3'] = self.stat_order(self.get_dates(3), list(df['product_id']), tp=1)
        df['order7'] = self.stat_order(self.get_dates(7), list(df['product_id']), tp=1)
        df['pay1'] = self.stat_order(self.get_dates(1), list(df['product_id']), tp=2)
        df['pay3'] = self.stat_order(self.get_dates(3), list(df['product_id']), tp=2)
        df['pay7'] = self.stat_order(self.get_dates(7), list(df['product_id']), tp=2)
        df.sort_values(by='sort', inplace=True)
        df.to_excel(self.writer, '汇总')
        df.to_sql('stat_total', self.sort_mysql, if_exists='append', index=False)
        self.writer.save()          # 保存excel
        self.write_data(df)         # 更新测试站mysql的sort值


# if __name__ == '__main__':
#     wv = ShoppingSort()
#     re_data = wv.calculate_sort('2018-12-11')
#     re_w = wv.weight_sort(re_data)
#     print(re_w)
#     for x in '7654321':
#         wv.save_data(str((datetime.datetime.today() - datetime.timedelta(days=int(x))).date()))
#
#     # wv.save_data(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))
#     wv.sort_run(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))
#     wv.write_excel()
