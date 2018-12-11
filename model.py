#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/16 14:59
# @Author  : zhangpeng
# @File    : model.py
# 说明     :  计算模型


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

        self.site = 'm.dwstyle.com'
        self.writer = pd.ExcelWriter('/root/project/data_files/stat_matomo.xls')

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

        return ret

    def cul_run(self, x_date):
        re_data = self.test_calculate_sort(x_date)
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
        # TODO: 增加数据生成excel文档
        re_data.to_excel(self.writer, sheet_name=str(x_date))
        self.writer.save()

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
            print(e_sql)
            self.mysql_conn.execute(e_sql)

            if n % 500 == 0:
                self.mysql_conn.commit()

        self.mysql_conn.commit()

    def weigth_avg(self, data):
        if sum(data) == 0:
            return 0.00

        ws = [1.11403, 1.4451, 1.75645, 2.05544, 2.33468, 2.6784, 2.98413]
        data_ws = [(k, v) for k, v in zip(data, ws) if k]
        v = [x for x, y in data_ws]
        w = [y for x, y in data_ws]
        if w:
            return round(np.average(v, weights=w), 7)
        else:
            return 0.00

    def save_data(self, x_date):
        df = self.cul_run(x_date)
        sql = '''select product_id from stat_space.sort_result;'''
        result = self.pgconn.execute(sql)
        product_list = set([int(x) for x in result.fetchall() if x])
        df_list = set([int(x) for x in df['product_id'] if x])
        new_p = df_list.difference(product_list)
        if new_p:
            ndp = pd.DataFrame(data=list(new_p), columns=['product_id'], dtype='int')
            ndp.to_sql('sort_result', self.pgconn, schema='stat_space', if_exists='append', index=False)

        week_map = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        # yesterday = datetime.date.today() - datetime.timedelta(days=1)
        day = datetime.datetime.strptime(x_date, '%Y-%m-%d').weekday()
        field = week_map[day]
        df[field] = round(df['value'], 7)
        df.fillna(0.00)
        sql = '''update stat_space.sort_result set {2}={1} where product_id={0};'''
        n = 0
        for p, v in zip(df['product_id'], df['value']):
            n = n + 1
            e_sql = sql.format(p, v, field)
            self.pgconn.execute(e_sql)
        print('today is ', field)

    def sort_run(self, x_date):
        sql = ('''select product_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday 
        from stat_space.sort_result;''')
        df = pd.read_sql(sql, self.pgconn)
        df['value'] = [self.weigth_avg([row['monday'], row['tuesday'], row['wednesday'], row['thursday'], row['friday'],
                                        row['saturday'], row['sunday']]) for index, row in df.iterrows()]
        df.sort_values(by='value', ascending=False, inplace=True)  # 按一列排序
        df['sort'] = [x for x in range(1, len(df.index) + 1)]
        print(df.head())
        df['date'] = x_date
        df.fillna(0.00)
        sql = '''update stat_space.sort_result set value={1},sort={2},date={3} where product_id={0};'''
        n = 0
        for p, v, s, d in zip(df['product_id'], df['value'], df['sort'], df['date']):
            n = n + 1
            e_sql = sql.format(p, v, s, d)
            self.pgconn.execute(e_sql)

    def write_excel(self):
        sql = ('''select * from stat_space.sort_result;''')
        df = pd.read_sql(sql, self.pgconn)
        df['value'].fillna(0.00)
        df['sort'].fillna(99999)
        df.to_excel(self.writer, '汇总')
        self.writer.save()

if __name__ == '__main__':
    wv = ShoppingSort()
    for x in '654321':
        wv.save_data(str((datetime.datetime.today() - datetime.timedelta(days=int(x))).date()))

    wv.sort_run(str((datetime.datetime.today() - datetime.timedelta(days=1)).date()))
    wv.write_excel()

