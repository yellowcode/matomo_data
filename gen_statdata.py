#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/16 14:21
# @Author  : zhangpeng
# @File    : gen_statdata.py
# 说明     : 生成模型数据


import copy
import re
from collections import Counter
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
        self.index_response = self.spider_index()

    def get_visuid(self, x_date):
        """
        获取当天用户id
        :param x_date: 日期
        :return:
        """
        ret = {}
        sql = ('''SELECT distinct uid FROM visit_details 
        WHERE to_char(to_timestamp(servertimestamp), 'yyyy-MM-dd')='{0}' and countrycode <> 'cn';''').format(x_date)
        result = self.pgconn.execute(sql)
        ret['ua'] = str(tuple([x[0] for x in result.fetchall()]))
        #
        # sql = ('''SELECT distinct uid FROM visit_details
        # WHERE to_char(to_timestamp(servertimestamp), 'yyyy-MM-dd')='{0}' and countrycode <> 'cn';''').format(x_date)
        # result = self.pgconn.execute(sql)
        # ret.append(str(tuple([x[0] for x in result.fetchall()])))

        return ret

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
        :return:
        """
        url = 'http://' + self.site + '/shopping/promotion_code'
        response = requests.get(url).json()

        return response

    def spider_index(self):
        hd = {'user-agent': ('Mozilla/5.0 (Linux; Android 8.1; EML-AL00 Build/HUAWEIEML-AL00; wv) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.143 '
                             'Crosswalk/24.53.595.0 XWEB/358 MMWEBSDK/23 Mobile Safari/537.36 '
                             'MicroMessenger/6.7.2.1340(0x2607023A) NetType/4G Language/zh_CN')}
        response = requests.get('https://' + self.site, headers=hd)
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
        ref_pms = list(set(['https://' + self.site + x for x in ref_pms]))
        for ul in ref_pms:
            response = requests.get(ul, headers=hd)
            response.encoding = 'utf-8'
            regs = re.findall('-p-(\d{4,7})\.html', response.text)
            index_pms[ul] = '-'.join([x for x in regs])
        index_pms['https://' + self.site + '/'] = '-'.join(index)

        for x in index_pms:
            index_pms[x] = [int(x) for x in re.findall('\d+', index_pms.get(x))]

        return index_pms

    def index_show(self, x_date, n_uids):
        """
        首页曝光
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        # 乘以？  当日action次数 | 当日访客次数,  当前选择action次数
        index_product = self.index_response.get('https://' + self.site + '/')
        sql = ('''SELECT count(1) as a_num FROM action where length(url)<={0} 
        and to_char(to_timestamp(action.timestamp), 'yyyy-MM-dd')='{1}' 
        and pid in {2};''').format(len('https:///' + self.site), x_date, n_uids)
        # sql = ('''SELECT count(1) as v_num FROM visit_details WHERE idsite={0}
        # and to_char(to_timestamp(servertimestamp), 'yyyy-MM-dd')='{1}';''').format(self.idsite, x_date)
        result = self.pgconn.execute(sql)
        num = result.fetchone()
        if num:
            ret = [{'product_id': int(x), 'index_show': num[0]} for x in index_product]
        else:
            ret = []

        return ret

    def promotion_show(self, x_date, n_uids):
        """
        活动曝光
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        promotion_map = copy.deepcopy(self.index_response)
        promotion_map.pop('https://' + self.site + '/')

        sql = ('''SELECT action.url, count(1) as num FROM action 
        WHERE to_char(to_timestamp(action.timestamp), 'yyyy-MM-dd')='{0}' and action.url in {1} and pid in {2}
        GROUP BY action.url;''').format(x_date, str(tuple(promotion_map.keys())), n_uids)
        result = self.pgconn.execute(sql)
        ret = []
        for val in result.fetchall():
            ret = ret + [{'product_id': int(x), 'promotion_show': val[1]} for x in promotion_map.get(val[0])]

        return ret

    def ad_show(self, x_date, n_uids):
        """
        广告着陆的首页
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        del_word = 'utm_source=|banner|id_sort|bg=|-c-p-'
        sql = ('''SELECT array_to_string(array_agg(product),','),count(1) as num FROM 
        public."event" ee WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and ee.url ~ '{0}.+?-c-' 
        and split_part(ee.url, 'html?', 2) ~ '{2}' and pid in {3}
        GROUP BY url,eventname,pageidaction;''').format(self.site, x_date, del_word, n_uids)
        result = self.pgconn.execute(sql)
        product_dict = {}
        for val in result.fetchall():
            st = [(k, int(val[1])) for k in val[0].split(',')]
            for k, v in st:
                if k in product_dict:
                    product_dict[k] = product_dict.get(k) + v
                else:
                    product_dict[k].update({k: v})

        return [{'product_id': int(k), 'ad_show': v} for k, v in product_dict]

    def search_show(self, x_date, n_uids):
        """
        获取搜索页的曝光
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        sql = ('''SELECT url, count(1) as num FROM action 
        WHERE url ~ '{0}.+?search' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and pid in {2}
        GROUP BY url''').format(self.site, x_date, n_uids)
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

            try:
                response = self.get_search(keyword, page)
            except Exception as e:
                print('search requests error: ', e)
                continue

            ret = ret + [{'product_id': int(x.get('product_id')), 'search_show': val[-1]} for x in response]

        return ret

    def ocl_click(self, x_date, key, n_uids):
        """
        下单、加购、收藏统计
        :param x_date: 日期
        :param key: 类别
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        flags = {'order_click': 'addOrder', 'cart_click': 'addCart', 'like_click': 'collect'}
        sql = ('''SELECT eventname, count(1) as num FROM event 
        WHERE eventaction='{0}' and url ~ '{1}' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{2}' 
        and pid in {3} GROUP BY eventname''').format(flags.get(key), self.site, x_date, n_uids)
        result = self.pgconn.execute(sql)
        return [{'product_id': int(x[0]), key: x[1]} for x in result.fetchall()]

    def total_detail_click(self, x_date, n_uids):
        """
        详情点击量统计
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        sql = ('''SELECT substring(aa.url from '-p-(\d+)\.html') as product_idv, count(1) as num FROM action aa 
        where aa.url ~ '{0}.+?-p-' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and pid in {2}
        GROUP BY product_idv HAVING substring(aa.url from '-p-(\d+)\.html') is not null;''').format(self.site, x_date, n_uids)
        result = self.pgconn.execute(sql)
        return [{'product_id': int(x[0]), 'total_detail_click': x[1]} for x in result.fetchall()]

    def list_show(self, x_date, n_uids):
        """
        列表曝光
        :param x_date: 日期
        :param n_uids: 存在uid序列
        :return: [{},{}]
        """
        del_word = 'utm_source=|banner|id_sort|bg=|-c-p-'
        sql = ('''SELECT array_to_string(array_agg(product),','),count(1) as num FROM 
        public."event" ee WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and ee.url ~ '{0}.+?-c-' 
        and not split_part(ee.url, 'html?', 2) ~ '{2}' and pid in {3} 
        GROUP BY url,eventname,pageidaction;''').format(self.site, x_date, del_word, n_uids)
        result = self.pgconn.execute(sql)
        product_dict = {}
        for val in result.fetchall():
            st = [(k, int(val[1])) for k in val[0].split(',')]
            for k, v in st:
                if k in product_dict:
                    product_dict[k] = product_dict.get(k) + v
                else:
                    product_dict[k].update({k: v})

        return [{'product_id': int(k), 'list_show': v} for k, v in product_dict]

    def list_click(self, x_date, n_uids):
        """
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        del_word = 'utm_source=|banner|id_sort|bg=|-c-p-'
        sql = '''SELECT pageidaction from action WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        and action.url in (select distinct tb.url from (SELECT url,eventname,pageidaction FROM public."event" ee 
        WHERE ee.url ~ '{0}.+?-c-' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        and not split_part(ee.url, 'html?', 2) ~ '{2}') as tb) 
        GROUP BY url,pageidaction;'''.format(self.site, x_date, del_word)
        result = self.pgconn.execute(sql)
        result = str(tuple([x[0] for x in result.fetchall()]))
        c_sql = '''SELECT substring(action.url from '-p-(\d+)\.html') as prod, pageidrefaction, count(1) as num 
        FROM action where to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{0}' and url ~ '{1}.+?-p-' and pid in {2} 
        GROUP BY prod, pageidrefaction HAVING pageidrefaction in {3}'''.format(x_date, self.site, n_uids, result)
        result = self.pgconn.execute(c_sql)
        return [{'product_id': int(x[0]), 'list_click': x[-1]} for x in result.fetchall() if x[0]]

    def ad_click(self, x_date, n_uids):
        """
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        del_word = 'utm_source=|id_sort|bg='
        sql = ('''SELECT pageidaction FROM action WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        and url in (SELECT DISTINCT url from (SELECT DISTINCT url,eventaction,eventname,pageidaction 
        FROM public."event" ee WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        and ee.url ~ '{0}.+?-c-\d+.+?(?:{2})') as tb) 
        GROUP BY url,pageidaction''').format(self.site, x_date, del_word)
        result = self.pgconn.execute(sql)
        result = str(tuple([x[0] for x in result.fetchall()]))
        c_sql = '''SELECT substring(action.url from '-p-(\d+)\.html') as prod, pageidrefaction, count(1) as num 
        FROM action where to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{0}' and url ~ '{1}.+?-p-' and pid in {2}
        GROUP BY prod, pageidrefaction HAVING pageidrefaction in {3} ORDER BY num DESC'''.format(x_date, self.site, n_uids, result)
        result = self.pgconn.execute(c_sql)
        return [{'product_id': int(x[0]), 'ad_click': x[-1]} for x in result.fetchall()]

    def search_click(self, x_date, n_uids):
        """
        搜索点击量
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        sql = '''SELECT substring(action.url from '-p-(\d+)\.html'), count(1) as num FROM action 
        WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and url ~ '{0}.+?-p-' 
        and action.pageidrefaction in (SELECT pageidaction FROM public.action ee 
        WHERE ee.url ~ '{0}.+?search' and to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}') and pid in {2} 
        GROUP BY substring(action.url from '-p-(\d+)\.html')'''.format(self.site, x_date, n_uids)
        result = self.pgconn.execute(sql)
        return [{'product_id': int(x[0]), 'search_click': x[1]} for x in result.fetchall()]

    def index_click(self, x_date, n_uids):
        """
        首页点击量
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        sql = '''SELECT substring(action.url from '-p-(\d+)\.html'), count(1) as num FROM action 
        WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and url ~ 'm.dwstyle.com.+?-p-' 
        and action.pageidrefaction=(SELECT pageidaction FROM action WHERE action.url='https://{0}/' LIMIT 1) 
        and pid in {2} GROUP BY substring(action.url from '-p-(\d+)\.html')'''.format(self.site, x_date, n_uids)
        result = self.pgconn.execute(sql)
        return [{'product_id': int(x[0]), 'index_click': x[1]} for x in result.fetchall()]

    def promotion_click(self, x_date, n_uids):
        """
        活动点击量
        :param x_date: 日期
        :param n_uids: 指定uid序列
        :return: [{},{}]
        """
        pm_word = 'banner|/sale'
        sql = '''SELECT substring(action.url from '-p-(\d+)\.html') as product_id, count(1) as num FROM action 
        WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' and url ~ '{0}.+?-p-' and action.pageidrefaction in 
        (SELECT pageidaction FROM action WHERE to_char(to_timestamp("timestamp"), 'yyyy-MM-dd')='{1}' 
        and action.url ~ '\?ref=.*?{2}' GROUP BY action.pageidaction) and pid in {3}
        GROUP BY substring(action.url from '-p-(\d+)\.html')'''.format(self.site, x_date, pm_word, n_uids)
        result = self.pgconn.execute(sql)
        return [{'product_id': int(x[0]), 'promotion_click': x[1]} for x in result.fetchall()]

    def data_class_stat(self, x_date, uids):
        """
        分类统计
        :param x_date:  日期
        :param uids:  访问者uid集合
        :return:
        """
        left_list_show = pd.DataFrame(self.list_show(x_date, uids))
        print('left_list_show')
        left_ad_show = pd.DataFrame(self.ad_show(x_date, uids))
        print('left_ad_show')
        # left_search_show = pd.DataFrame(self.search_show(x_date, uids))
        # print('left_search_show')
        left_index_show = pd.DataFrame(self.index_show(x_date, uids))
        print('left_index_show')
        left_promotion_show = pd.DataFrame(self.promotion_show(x_date, uids))
        print('left_promotion_show')

        right_total_detail_click = pd.DataFrame(self.total_detail_click(x_date, uids))
        print('right_total_detail_click')
        right_list_click = pd.DataFrame(self.list_click(x_date, uids))
        print('right_list_click')
        right_ad_click = pd.DataFrame(self.ad_click(x_date, uids))
        print('right_ad_click')
        right_search_click = pd.DataFrame(self.search_click(x_date, uids))
        print('right_search_click')
        right_promotion_click = pd.DataFrame(self.promotion_click(x_date, uids))
        print('right_promotion_click')
        right_index_click = pd.DataFrame(self.index_click(x_date, uids))
        print('right_index_click')

        right_order_click = pd.DataFrame(self.ocl_click(x_date, 'order_click', uids))
        print('right_order_click')
        right_cart_click = pd.DataFrame(self.ocl_click(x_date, 'cart_click', uids))
        print('right_cart_click')
        right_like_click = pd.DataFrame(self.ocl_click(x_date, 'like_click', uids))
        print('right_like_click')

        pds = [
            left_list_show,
            left_ad_show,
            # left_search_show,
            left_index_show,
            left_promotion_show,
            right_total_detail_click,
            right_list_click,
            right_ad_click,
            right_search_click,
            right_promotion_click,
            right_index_click,
            right_order_click,
            right_cart_click,
            right_like_click
        ]

        product_id = []
        for x in range(len(pds)):
            if pds[x].empty:
                continue

            keys = list(pds[x].columns)
            keys.remove('product_id')
            pds[x] = pds[x].groupby('product_id').agg({keys[0]: 'sum'}).reset_index()
            product_id = product_id + [{'product_id': int(n)} for n in set(pds[x]['product_id'])]

        result = pd.DataFrame(product_id)
        for px in pds:
            result = pd.merge(result, px, how='left', on='product_id').reset_index(drop='index')

        result['date'] = str(x_date)
        result.drop_duplicates(keep='first', inplace=True)
        result.to_sql('shopping_params', self.pgconn, schema='stat_space', if_exists='append', index=False)

    def gen_sql_stat(self, x_date):
        """
        生成统计值
        :return:
        """
        c_uids = self.get_visuid(x_date)
        for c in c_uids:
            self.data_class_stat(x_date, c_uids.get(c))


if __name__ == '__main__':
    import datetime
    sd = StatData()
    sd.gen_sql_stat(str((datetime.datetime.today() - datetime.timedelta(days=2)).date()))