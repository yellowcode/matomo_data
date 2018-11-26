#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2018/11/14 14:21
# @Author  : zhangpeng
# @File    : download_data.py
# 说明     : 下载matomo数据到数据库


import time
import datetime
import random
import json
import requests
import pandas as pd
import uuid
from pgsql_pool import PgsqlConn


class MatomoApi(object):

    def __init__(self):
        self.site = 'http://m.dwstyle.com'
        self.dv_type = 'phone'
        self.idsite = 2
        self.__urls = {'detail': 'https://2.zerostats.com/index.php?module=API&method=Live.getLastVisitsDetails&filter_limit=500&filter_offset={3}&format=json&idSite={2}&period=day&date={0}&token_auth={1}',
                       'visitor': 'https://2.zerostats.com/index.php?module=API&method=Live.getVisitorProfile&idSite={2}&filter_limit=500&filter_offset={3}&format=JSON&token_auth={1}&visitorId={0}',
                       }

        pgdb = PgsqlConn()
        self.pgconn = pgdb.pgsql_conn()
        self.__token = '0f8c3d42fb040f1950e50a24264cdf56'
        self.hd = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'}
        self.detail_struct = ['action', 'event', 'goal', 'ecommerceabandonedcart', 'ecommerceorder', 'itemdetails']  # itemdetails必须为末尾元素
        self.fields = self.get_table_column(self.detail_struct + ['visit_details', ])


    def get_table_column(self, tables):
        """
        :param tables: 表名
        :return:
        """
        sql = '''select table_name, string_agg(column_name, ',') as column_name from information_schema.columns where table_schema='public' and "table_name" in {0} GROUP BY table_name;'''
        result = self.pgconn.execute(sql.format(str(tuple(tables))))
        ret = dict([(x[0], x[1].split(',')) for x in result.fetchall()])
        for x in ret:
            ret.get(x).remove('id')

        return ret

    def get_max_vid(self, vid):
        """
        :param vid: vid
        :return:
        """
        sql = '''SELECT max(aa."timestamp") as ax FROM {0} aa WHERE aa.pid in (SELECT cc.uid FROM visit_details cc WHERE cc.visitorid='{1}');'''
        ret = {}
        for tb in self.detail_struct[:-1]:
            result = self.pgconn.execute(sql.format(tb, vid))
            rep = result.fetchone()
            ret[tb] = rep

        return ret

    def dl_details(self, t):
        """
        :param t:  日期，例 2018-11-14
        :return: [{ }, { }]
        """
        n_page = 0
        while True:
            n_page += 1
            if n_page < 25: continue
            detail_url = self.__urls.get('detail').format(t, self.__token, self.idsite, 500 * n_page)
            print(detail_url)
            try:
                response = requests.get(detail_url, headers=self.hd).json()
                if not response:
                    break
                else:
                    yield response
            except Exception as e:
                print('dl_details exceptions: ', e)
                break


    def dl_visitor(self, vid):
        """
        :param vid:  visitorId
        :return: { }
        """
        detail_url = self.__urls.get('visitor').format(vid, self.__token, self.idsite)
        try:
            response = requests.get(detail_url, headers=self.hd).json()
        except requests.exceptions as e:
            response = []
            print(e)

        return response

    def parse_json(self, dct):
        """
        解析单条json
        :param dct: json data
        :return: [(),(),()...]
        """
        ret = {}
        visit_uid = str(uuid.uuid1()).replace('-', '')
        actiondetails = dct.pop('actionDetails')
        ret['visit_details'] = dict([(k.lower(), v) for k, v in dct.items() if k.lower() in self.fields.get('visit_details')])
        ret['visit_details'].update({'uid': visit_uid})

        # sort_val = self.get_max_vid(ret.get('visitorid'))     # 访问者的访问事件记录递增
        for x in self.detail_struct:
            ret[x] = []

        sort_val = 1
        for dt in actiondetails:
            tp = dt.pop('type').lower()
            if tp not in self.detail_struct:
                continue

            action_uid = str(uuid.uuid1()).replace('-', '')
            customvariables = dt.pop('customVariables') if dt.get('customVariables') else []
            for strn in customvariables:
                dt['customVariables' + customvariables.get(strn).get('customVariablePageName' + strn)] = customvariables.get(strn).get('customVariablePageValue' + strn)

            if dt.get('customVariables_pkc'):
                if isinstance(dt.get('customVariables_pkc'), list):
                    dt['customVariables_pkc'] = '_'.join(dt.get('customVariables_pkc'))
                dt['itemcategory'] = dt.pop('customVariables_pkc')

            if tp in ('ecommerceabandonedcart', 'ecommerceOrder') and dt.get('itemDetails'):
                itemdetails = dt.pop('itemDetails')
                [x.update({'pid': action_uid}) for x in itemdetails]
                for ix in itemdetails:
                    ix = dict([(k.lower(), v) for k, v in ix.items() if k.lower() in self.fields.get('itemdetails')])
                    ret['itemdetails'].append(ix)

            dt = dict([(k.lower(), v) for k, v in dt.items() if k.lower() in self.fields.get(tp)])
            dt.update({'uid': action_uid, 'pid': visit_uid, 'record_id': sort_val})
            sort_val += 1
            ret[tp].append(dt)

        return ret

    def clean_dt(self, dct):
        """
        json脏数据清洗
        :param dct: json
        :return:  dct
        """
        jdata = json.dumps(dct)
        jdata.replace('"[', '[').replace(']"', ']').replace('"none"', '')

        return json.loads(jdata)

    def parse_detail(self, ldata):
        """
        :param ldata: [{ },{ }]
        :return:
        """
        datas = dict([(x, []) for x in self.detail_struct + ['visit_details', ]])
        for ldt in ldata:
            try:
                ldt = self.clean_dt(ldt)
                ret_json = self.parse_json(ldt)
            except Exception as e:
                print(e)
                continue

            for x in ret_json:
                if x == 'visit_details':
                    datas[x].append(ret_json.get(x))
                else:
                    datas[x].extend(ret_json.get(x))

        for key_name in datas:
            if not datas.get(key_name):
                continue
            df = pd.DataFrame(datas.get(key_name))
            df.to_sql(key_name, self.pgconn, if_exists='append', index=False)

        return True

    def get_category(self):
        """
        获取站点商品分类
        :return:
        """
        category_api = self.site + '/shopping/category'
        response = requests.get(category_api).json()
        result = response.get('result')
        if len(result) == 1:
            return result[0].get('subcategory')
        else:
            return list(result.keys())

    def get_product(self, category_id, sort_type=1, page=None):
        """
        :param category_id:  分类id
        :param page: 请求页
        :param sort_type: 请求类型
        :return:
        """
        if isinstance(page, int):
            product_api = self.site + '/shopping/subcategory_shopping?category_id={0}&page={1}&sort_type={2}'.format(
                category_id, page, sort_type)
        else:
            product_api = self.site + '/shopping/subcategory_shopping?category_id={0}&sort_type={1}'.format(
                category_id, sort_type)

        print(product_api)
        response = requests.get(product_api, headers=self.hd).json()
        return response.get('result')

    def get_category_all_product(self, ctg):
        """
        分页获取整个分类的商品
        :param ctg:  分类
        :return:
        """
        ret = []
        n = 1
        data = self.get_product(ctg, page=n)
        while data:
            print(ctg, ': ', n)
            ret = ret + data
            data = self.get_product(ctg, page=n)
            time.sleep(random.choice(list(range(5))))
            n += 1

        return ret

    def save_product(self):
        """
        整个站点的所有产品数据比较， 数据量大的时候需要重新处理
        :return:
        """
        ctg = self.get_category()
        data = []
        for cg in ctg:  # 根据大类直接获取整个大类的所有商品
            data = data + self.get_category_all_product(cg)
        new_df = pd.DataFrame(data)
        new_df['site'] = self.site.replace('http://', '')
        new_df['timestamp'] = int(time.time())
        new_df.to_sql('product_record', self.pgconn, schema='public', if_exists='append', index=False)
        new_df['drive_type'] = self.dv_type
        new_df.drop('price', axis=1, inplace=True)
        new_df.drop('timestamp', axis=1, inplace=True)

        sql = '''select * from stat_space.product;'''
        old_df = pd.read_sql(sql, self.pgconn)
        old_df.drop('id', axis=1, inplace=True)

        mdf = pd.merge(new_df, old_df, on=list(new_df.columns), how='inner')  # 找出新纪录和数据库相同的记录
        new_df.drop(new_df.product_id.isin(list(mdf['product_id'])).index, inplace=True)  # drop掉新纪录和数据库相同的记录

        if not new_df.empty:
            dsql = '''delete from stat_space.product where product_id in {0}'''.format(str(tuple(new_df['product_id'])))
            self.pgconn.execute(dsql)
            new_df.to_sql('product', self.pgconn, schema='stat_space', if_exists='append', index=False)

    def n_run(self, x_days: int):
        """
        :param x_days: 从今天起的前几天
        :return:
        """
        print(datetime.datetime.now().strftime('%y-%M-%d %H:%M:%S'))
        x_date = (datetime.datetime.today() - datetime.timedelta(days=x_days)).date()
        while x_date < datetime.datetime.today().date():
            print(x_date)
            dl_ret = self.dl_details(str(x_date))
            ln = 1
            for lx in dl_ret:
                self.parse_detail(lx)      # 解析matomo数据
                print(ln * 500)
                ln += 1
            x_date += datetime.timedelta(days=1)
        print(datetime.datetime.now().strftime('%y-%M-%d %H:%M:%S'))

    def run(self, x_date):
        """
        :param x_date: 日期
        :return:
        """
        # print(datetime.datetime.now().strftime('%y-%M-%d %H:%M:%S'))
        # dl_ret = self.dl_details(str(x_date))
        # ln = 1
        # for lx in dl_ret:
        #     self.parse_detail(lx)  # 解析matomo数据
        #     print(ln * 500)
        #     ln += 1
        # print(datetime.datetime.now().strftime('%y-%M-%d %H:%M:%S'))


if __name__ == '__main__':
    mapi = MatomoApi()
    # mapi.n_run(9)
    mapi.run((datetime.datetime.today() - datetime.timedelta(days=1)).date())
    mapi.save_product()  # 获取商城整站product数据
