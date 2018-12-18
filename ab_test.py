#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/12/18 11:43
# @Author  : zhangpeng
# @File    : ab_test.py
# 说明     :  ab测试


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


class AbTest(object):

    def __init__(self):
        self.site = 'https://m.dwstyle.com'
        self.dv_type = 'phone'
        self.idsite = 7
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
        sql = ('''select table_name, string_agg(column_name, ',') as column_name 
        from information_schema.columns where table_schema='public' and "table_name" in {0} 
        GROUP BY table_name;''')
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
        sql = ('''SELECT max(aa."timestamp") as ax FROM {0} aa WHERE aa.pid in 
        (SELECT cc.uid FROM visit_details cc WHERE cc.visitorid='{1}');''')
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
            detail_url = self.__urls.get('detail').format(t, self.__token, self.idsite, 500 * n_page)
            n_page += 1
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
        cvs = dct.get('customVariables')
        ret['user_type'] = cvs.get('customVariableValue1') if cvs else ''

        # sort_val = self.get_max_vid(ret.get('visitorid'))     # 访问者的访问事件记录递增
        for x in self.detail_struct:
            ret[x] = []

        sort_val = 1
        for dt in actiondetails:
            tp = dt.pop('type').lower()
            if tp != 'goal':
                continue

            if tp not in self.detail_struct:
                continue

            if tp == 'event' and dt.get('eventName') and 'product' in dt.get('eventName'):
                regs = re.findall('\d+', dt.get('eventName'))
                dt['eventname'] = regs[0]
                dt['product'] = ','.join(regs[1:])

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
            df.to_sql(key_name, self.pgconn, schema='abtest', if_exists='append', index=False)

        return True

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

    def shopping_change(self, x_date):
        sql = ''''''


if __name__ == '__main__':
    abtest = AbTest()
    # abtest.n_run(9)
    abtest.run((datetime.datetime.today() - datetime.timedelta(days=1)).date())

