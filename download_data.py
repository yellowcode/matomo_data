
#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2018/11/14 14:21
# @Author  : zhangpeng
# @File    : download_data.py
# 说明     : 下载matomo数据到数据库


import json
import requests
import pandas as pd
import uuid
from pgsql_pool import PgsqlConn


class MatomoApi(object):

    def __init__(self):
        pgdb = PgsqlConn()
        self.pgconn = pgdb.pgsql_conn()
        self.hd = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'}
        self.__urls = {'detail': 'https://2.zerostats.com/index.php?module=API&method=Live.getLastVisitsDetails&format=json&idSite=1&period=day&date={0}&token_auth=0f8c3d42fb040f1950e50a24264cdf56',
                       'visitor': 'https://2.zerostats.com/index.php?module=API&method=Live.getVisitorProfile&idSite=1&format=JSON&token_auth=0f8c3d42fb040f1950e50a24264cdf56&visitorId={0}',
                       }
        self.detail_struct = ['action', 'event', 'goal', 'ecommerceabandonedcart', 'ecommerceorder', 'itemdetails']
        self.fields = self.get_table_column(self.detail_struct + ['visit_details', ])


    def get_table_column(self, tables):
        """
        :param tablename: 表名
        :return:
        """
        sql = '''select table_name, string_agg(column_name, ',') as column_name from information_schema.columns where table_schema='public' and "table_name" in {0} GROUP BY table_name;'''
        result = self.pgconn.execute(sql.format(str(tuple(tables))))
        ret = dict([(x[0], x[1].split(',')) for x in result.fetchall()])
        for x in ret:
            ret.get(x).remove('id')

        return ret

    def dl_details(self, t):
        """
        :param t:  日期，例 2018-11-14
        :return: [{ }, { }]
        """
        detail_url = self.__urls.get('detail').format(t)
        try:
            response = requests.get(detail_url, headers=self.hd).json()
        except requests.exceptions as e:
            response = []
            print(e)

        return response

    def dl_visitor(self, vid):
        """
        :param vid:  visitorId
        :return: { }
        """
        detail_url = self.__urls.get('visitor').format(vid)
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
        # if isinstance(dct.get('actionDetails'), list):
        actiondetails = dct.pop('actionDetails')
        ret['visit_details'] = dict([(k.lower(), v) for k, v in dct.items() if k.lower() in self.fields.get('visit_details')])
        ret['visit_details'].update({'uid': visit_uid})
        for x in self.detail_struct:
            ret[x] = []
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
            dt.update({'uid': action_uid, 'pid': visit_uid})
            ret[tp].append(dt)

        return ret

    def clean_dt(self, dct):
        """
        json脏数据清洗
        :param dct: json
        :return:  dct
        """
        jdata = json.dumps(dct)
        jdata.replace('"[', '[').replace(']"', ']')

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
            df = pd.DataFrame(datas.get(key_name))
            df.to_sql(key_name, self.pgconn, if_exists='append', index=False)

        return datas


if __name__ == '__main__':
    ss = MatomoApi()
    for x_date in ["2018-10-29","2018-10-30","2018-10-31","2018-11-01","2018-11-02","2018-11-05","2018-11-06","2018-11-07","2018-11-08","2018-11-09","2018-11-12","2018-11-13"]:
    # for x_date in ["2018-11-05",]:
        print(x_date)
        test = ss.dl_details(x_date)
        aa = ss.parse_detail(test)
