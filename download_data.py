#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/14 14:21
# @Author  : zhangpeng
# @File    : download_data.py
# 说明     : 下载matomo数据到数据库


import pathlib
import time
import datetime
import random
import re
import json
import requests
import pandas as pd
import uuid
from pgsql_pool import PgsqlConn


class MatomoApi(object):

    def __init__(self):
        self.site = 'https://m.dwstyle.com'
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

    def file_dumps(self, fname, text):
        pathlib.Path('/'.join(fname.split('/')[:-1])).mkdir(parents=True, exist_ok=True)
        with open(fname, 'w') as fp:
            fp.write(json.dumps(text, ensure_ascii=False))

    def dl_details(self, t):
        """
        :param t:  日期，例 2018-11-14
        :return: [{ }, { }]
        """
        n_page = 0
        while True:
            n_page += 1
            detail_url = self.__urls.get('detail').format(t, self.__token, self.idsite, 500 * n_page)
            print(detail_url)
            try:
                response = requests.get(detail_url, headers=self.hd).json()
                if not response:
                    break
                else:
                    self.file_dumps('/root/project/mdata/{0}/{1}_{2}.txt'.format(t, 500 * (n_page - 1), 500 * n_page), response)
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

    def get_order_product(self, order_id):
        """
        获取订单商品状态
        :param order_id: 订单id
        :return:
        """
        data = []
        url = 'https://m.dwstyle.com/shopping/order_products?order_sn=' + order_id
        response = requests.get(url)
        if response.status_code == 200:
            response = response.json()
            if response.get('result'):
                for dt in response.get('result'):
                    for prs in dt.get('order_products'):
                        tmp = {}
                        tmp['order_id'] = dt.get('order_id')
                        tmp['order_sn'] = dt.get('order_sn')
                        tmp['order_status'] = dt.get('order_status')
                        tmp['product_id'] = prs.get('id')
                        tmp['qty'] = prs.get('qty')
                        tmp['price'] = prs.get('price')
                        data.append(tmp)

        return data

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
        new_df['site'] = self.site.replace('https://', '')
        new_df['timestamp'] = int(time.time())
        new_df.to_sql('product_record', self.pgconn, schema='public', if_exists='append', index=False)
        new_df['drive_type'] = self.dv_type
        new_df.drop('price', axis=1, inplace=True)
        new_df.drop('timestamp', axis=1, inplace=True)

        sql = '''select * from stat_space.product;'''
        old_df = pd.read_sql(sql, self.pgconn)
        old_df.drop('id', axis=1, inplace=True)

        for k, v in zip(old_df.columns, old_df.dtypes):
            new_df[k] = new_df[k].astype(v)
        mdf = pd.merge(new_df, old_df, on=list(new_df.columns), how='inner')  # 找出新纪录和数据库相同的记录
        new_df.drop(new_df.product_id.isin(list(mdf['product_id'])).index, inplace=True)  # drop掉新纪录和数据库相同的记录

        if not new_df.empty:
            dsql = '''delete from stat_space.product where product_id in {0}'''.format(str(tuple(new_df['product_id'])))
            self.pgconn.execute(dsql)
            new_df.to_sql('product', self.pgconn, schema='stat_space', if_exists='append', index=False)

        # 勾选页中不存在于产品库中的数据
        self.check_spider_product()
        print('new index product_id ok')

    def n_run(self, x_days: int):
        """
        :param x_days: 从今天起的前几天
        :return:
        """
        print(datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S'))
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
        print(datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S'))

    def run(self, x_date):
        """
        :param x_date: 日期
        :return:
        """
        print(datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S'))
        dl_ret = self.dl_details(str(x_date))
        ln = 1
        for lx in dl_ret:
            self.parse_detail(lx)  # 解析matomo数据
            print(ln * 500)
            ln += 1
        print(datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S'))

    def spider_index(self):
        hd = {'user-agent': ('Mozilla/5.0 (Linux; Android 8.1; EML-AL00 Build/HUAWEIEML-AL00; wv) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.143 '
                             'Crosswalk/24.53.595.0 XWEB/358 MMWEBSDK/23 Mobile Safari/537.36 '
                             'MicroMessenger/6.7.2.1340(0x2607023A) NetType/4G Language/zh_CN')}
        response = requests.get(self.site, headers=hd)
        response.encoding = 'utf-8'
        regs = re.findall('<a href="(.+?)"[^>]*>', response.text)

        index_pms = {}
        index = []
        ref_pms = []
        for x in regs:
            flag = re.findall('home|button|leimu', x.split('ref')[-1])
            if '-p-' in x:
                index.append(x.split('.html')[0].split('-p-')[-1])
            elif 'ref' in x and '-c-' not in x and not flag:
                ref_pms.append(x)
        ref_pms = ref_pms + ['/new-entry.html', '/sale.html']
        ref_pms = list(set([self.site + x if x[-1] == '/' else self.site + x + '/' for x in ref_pms]))
        for ul in ref_pms:
            response = requests.get(ul, headers=hd)
            response.encoding = 'utf-8'
            regs = re.findall('<a href="(.+?)"[^>]*>', response.text)
            index_pms[ul] = '-'.join([x.split('.html')[0].split('-p-')[-1] for x in regs if '-p-' in x])
        index_pms[self.site + '/'] = '-'.join(index)

        for x in index_pms:
            index_pms[x] = [int(x) for x in re.findall('\d+', index_pms.get(x))]

        return index_pms

    def check_spider_product(self):
        sql = '''SELECT DISTINCT product_id FROM stat_space.product;'''
        result = self.pgconn.execute(sql)
        result = set([x[0] for x in result.fetchall()])
        resp = self.spider_index()
        datas = []
        [datas.extend(v) for k, v in resp.items()]
        datas = set(datas)
        diff_data = datas.difference(result)

        url = '{0}/shopping/detail_product?product_id={1}'
        ret = []
        for proid in diff_data:
            try:
                response = requests.get(url.format(self.site, proid)).json()
            except Exception as e:
                print('detail_product req error: ', proid, ': ', e)
                continue

            ret.append(response.get('result'))

        if not ret:
            return

        df = pd.DataFrame(ret)
        df['site'] = self.site.replace('http://', '')
        df['timestamp'] = int(time.time())
        df.to_sql('product_record', self.pgconn, schema='public', if_exists='append', index=False)
        df['drive_type'] = self.dv_type
        df.drop('price', axis=1, inplace=True)
        df.drop('timestamp', axis=1, inplace=True)
        df.to_sql('product', self.pgconn, schema='stat_space', if_exists='append', index=False)

    def save_order_product(self, x_date):
        sql = ('''SELECT substring(url from 'payment-(\d+)-') as order_id, count(1) as num FROM goal 
        where to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and goalname='下订单' and url ~ '{0}'
        GROUP BY order_id;''').format(self.site.replace('https://', ''), x_date)
        result = self.pgconn.execute(sql)
        ret = []
        order_sn = []
        n = 0
        for val in result.fetchall():
            n = n + 1
            # order_sn = order_sn + [val[0]] * val[1]
            order_sn.append(val[0])
            if n % 10 == 0:
                try:
                    resp = self.get_order_product(','.join(order_sn))
                except Exception as e:
                    print('order product data error: ', e)
                    resp = []
                ret = ret + resp
                order_sn = []

        df = pd.DataFrame(ret)
        df['date'] = x_date
        df.to_sql('product_order', self.pgconn, if_exists='append', index=False)



# if __name__ == '__main__':
#     mapi = MatomoApi()
#     # mapi.n_run(9)
#     mapi.run((datetime.datetime.today() - datetime.timedelta(days=1)).date())
#     mapi.save_product()  # 获取商城整站product数据
#     q = mapi.spider_index()
#     print(q)

