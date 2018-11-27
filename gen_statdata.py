#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/16 14:21
# @Author  : zhangpeng
# @File    : gen_statdata.py
# 说明     : 生成模型数据


import pandas as pd
import requests
from pgsql_pool import PgsqlConn


class StatData(object):
    """
    生成模型所用数据
    """
    def __init__(self):
        pgdb = PgsqlConn()
        self.pgconn = pgdb.pgsql_conn()
        self.site = 'm.dwstyle.com'
        self.idsite = 1
        self.sort_map = {
            'newness_desc': 2,
            'price_asc': 3,
            'price_desc': 4
        }

    def get_product(self, category_id, sort_type=1, page=None):
        """
        :param category_id:  分类id
        :param page: 请求页
        :param sort_type: 请求类型
        :return:
        """
        if isinstance(page, int):
            product_api = 'http://' + self.site + '/shopping/subcategory_shopping?category_id={0}&page={1}&sort_type={2}'.format(
                category_id, page, sort_type)
        else:
            product_api = 'http://' + self.site + '/shopping/subcategory_shopping?category_id={0}&sort_type={1}'.format(
                category_id, sort_type)

        response = requests.get(product_api)
        if response.status_code == 200:
            response = response.json()
            if response.get('result'):
                return response.get('result')
            else:
                return []
        else:
            return []

    def get_search(self, keyword, page):
        """
        请求搜索页数据
        :param keyword: 搜索关键词
        :param page: 搜索页当前页数
        :return:
        """
        search_api = 'http://' + self.site + '/shopping/search?keyword={0}&page={1}'.format(keyword, page)
        response = requests.get(search_api)
        if response.status_code == 200:
            response = response.json()
            if response.get('result'):
                return response.get('result')
            else:
                return []
        else:
            return []

    def get_promotion_code(self):
        """
        获取勾选code值
        :param site: 站点域名
        :return:
        """
        url = 'http://' + self.site + '/shopping/promotion_code'
        response = requests.get(url).json()

        return response

    def _test_get_promotion_code(self):
        """
        伪造code值数据
        :return:
        """
        return {
            "code": 200,
            "msg": "Success",
            "promotion": {
                "banner1": "banner",
                "index-banner1": "m-index-banner1",
                "index-banner2": "m-index-banner2",
                "index-banner3": "m-index-banner3",
                "index-banner4": "m-index-banner4"
            },
            "catagory_show": [
                4671,
                4664,
                4679,
                4686,
                4708
            ]
        }

    def get_promotion_product(self, code):
        """
        获取code值对应的产品
        :param x_date: 日期
        :param code: 勾选code值
        :return:
        """
        url = 'http://' + self.site + '/shopping/index_shopping?code={0}'.format(code)
        response = requests.get(url).json()

        return response.get('result')

    def index_show(self, x_date):
        """
        首页曝光
        :param x_date: 日期
        :return:
        """
        response = self.get_promotion_code()
        ret = []
        for ctg in response.get('catagory_show'):
            data = self.get_product(category_id=ctg, page=1, sort_type=1)
            ret = ret + data[:10]

        # TODO: 乘以？  当日action次数 | 当日访客次数
        sql = ('''SELECT count(1) as a_num FROM action where length(url) <= length('http:///' + {0}) 
        and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}';''').format(self.site, x_date)
        # sql = ('''SELECT count(1) as v_num FROM visit_details WHERE idsite={0}
        # and to_char(to_timestamp(servertimestamp), 'yyyy-MM-dd')='{1}';''').format(self.idsite, x_date)
        result = self.pgconn.execute(sql)
        num = result.fetchone()
        ret = [{'product_id': x.get('product_id'), 'index_show': num} for x in ret]

        return ret

    def promotion_show(self, x_date):
        """
        活动曝光
        :param x_date: 日期
        :return:
        """
        response = self.get_promotion_code()
        ret = []
        pms = response.get('promotion')

        sql = ('''SELECT split_part(url, 'ref=', 2), count(1) as num FROM action 
        WHERE split_part(url, 'ref=', 2) in {0} and url ~ '{1}' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{2}' 
        GROUP BY split_part(url, 'ref=', 2)''').format(str(tuple(pms.keys())), self.site, x_date)
        result = self.pgconn.execute(sql)
        data = dict([result.fetchall()])

        for code in pms:
            response = self.get_promotion_product(pms.get(code))
            ret = ret + [{'product_id': x.get('product_id'), 'promotion_show': data.get(code)} for x in response]

        return ret

    def ad_show(self, x_date):
        pass

    def search_show(self, x_date):
        """
        获取搜索页的曝光
        :param x_date:
        :return:
        """
        sql = ('''SELECT url, count(1) FROM action 
        WHERE url ~ '{0}.+?search' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        GROUP BY url''').format(self.site, x_date)
        result = self.pgconn.execute(sql)

        ret = []
        for val in result.fetchall():
            tmp = val[0].split('/')[-1].split('?page=')
            if len(tmp) > 1:
                keyword = tmp[0]
                page = tmp[1]
            else:
                keyword = tmp[0]
                page = 1

            print(keyword, '---', page)
            response = self.get_search(keyword, page)
            ret = ret + [{'product_id': x.get('product_id'), 'search_show': val[-1]} for x in response]

        return ret

    def list_click(self, x_date):
        """
        列表曝光
        :param x_date:
        :return:
        """
        sql = ('''SELECT split_part(url, '?', 1) as url,eventaction,eventname,count(1) as num FROM public."event" ee 
        WHERE ee.url ~ '-c-' and ee.url ~ '{0}' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        GROUP BY split_part(url, '?', 1),eventaction,eventname;''').format(self.site, x_date)
        result = self.pgconn.execute(sql)
        ret = []
        for val in result.fetchall():
            print(val)
            try:
                stp = 1
                for tp in self.sort_map:
                    if tp in val[0]:
                        stp = self.sort_map.get(tp)
                        print('stp: ', stp)
                    else:
                        stp = 1
                response = self.get_product(category_id=int(val[1]), page=int(val[2]), sort_type=stp)
                ret = ret + [{'product_id': int(x.get('product_id')), 'list_show': val[-1]} for x in response]
            except Exception as e:
                print('list_click event url error: ', e)
                continue

        # sql = ('''SELECT aa.url,count(1) as num FROM action aa, event ee
        # WHERE aa.url ~ '{0}.+?-c-' and to_char(to_timestamp(aa.timestamp), 'yyyy-MM-dd')='{1}'
        # and aa.url not in (SELECT ee.url FROM event ee) GROUP BY aa.url''').format(self.site, x_date)
        # result = self.pgconn.execute(sql)
        # for val in result.fetchall():
        #     stp = 1
        #     for tp in self.sort_map:
        #         if tp in val[0]:
        #             stp = self.sort_map.get(tp)
        #         else:
        #             stp = 1
        #     try:
        #         c = val[0].split('-c-')[-1].split('.html')
        #         reponse = self.get_product(category_id=int(c), page=1, sort_type=stp)
        #         ret = ret + [{'product_id': x.get('product_id'), 'list_show': val[-1]} for x in reponse]
        #     except Exception as e:
        #         print('list_click action url error: ', e)
        #         continue

        return ret

    def ocl_click(self, x_date, key):
        """
        下单、加购、收藏统计
        :param x_date: 日期
        :param key: 类别
        :return:
        """
        flags = {'order_click': 'addOrder', 'cart_click': 'addCart', 'like_click': 'collect'}
        sql = ('''SELECT eventname, count(1) as num FROM event 
        WHERE eventaction='{0}' and url ~ '{1}' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{2}' 
        GROUP BY eventname''').format(flags.get(key), self.site, x_date)
        result = self.pgconn.execute(sql)
        return [{'product_id': x[0], key: x[1]} for x in result.fetchall()]

    def detail_click(self, x_date):
        """
        详情点击量统计
        :param x_date: 日期
        :return:
        """
        sql = ('''SELECT substring(aa.url from '-p-(\d+)\.html') as product_id, count(1) as num FROM action aa 
        where aa.url ~ '{0}.+?-p-' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        GROUP BY substring(aa.url from '-p-(\d+)\.html');''').format(self.site, x_date)
        result = self.pgconn.execute(sql)
        return [{'product_id': int(x[0]), 'detail_click': x[1]} for x in result.fetchall()]

    def gen_sql_stat(self, x_date):
        """
        生成统计值
        :return:
        """
        left_list_click = pd.DataFrame(self.list_click(x_date))
        # right_index_show = pd.DataFrame(self.index_show(x_date))
        # right_promotion_show = pd.DataFrame(self.promotion_show(x_date))
        # right_ad_show = pd.DataFrame(self.ad_show(x_date))
        right_search_show = pd.DataFrame(self.search_show(x_date))
        right_detail_click = pd.DataFrame(self.detail_click(x_date))
        right_order_click = pd.DataFrame(self.ocl_click(x_date, 'order_click'))
        right_cart_click = pd.DataFrame(self.ocl_click(x_date, 'cart_click'))
        right_like_click = pd.DataFrame(self.ocl_click(x_date, 'like_click'))

        pds = [
            left_list_click,
            # right_index_show,
            # right_promotion_show,
            # right_ad_show,
            right_search_show,
            right_detail_click,
            right_order_click,
            right_cart_click,
            right_like_click
        ]

        product_id = []
        for x in range(len(pds)):
            keys = list(pds[x].columns)
            keys.remove('product_id')
            pds[x] = pds[x].groupby('product_id').agg({keys[0]: 'sum'}).reset_index()
            product_id = product_id + [{'product_id': int(n)} for n in set(pds[x]['product_id'])]

        result = pd.DataFrame(product_id)
        for px in pds:
            result = pd.merge(result, px, how='left', on='product_id').reset_index(drop='index')

        result.to_sql('shopping_params', self.pgconn, schema='stat_space', if_exists='append', index=False)


if __name__ == '__main__':
    sd = StatData()
    sd.gen_sql_stat('2018-11-25')
    # q = sd.search_show('2018-11-25')
