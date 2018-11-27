#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/16 14:59
# @Author  : zhangpeng
# @File    : model.py
# 说明     :  计算模型


import numpy as np


class ShoppingSort(object):

    def __init__(self):
        pass

    def wilson_score(self, pos, total, p_z=2.0):


        """
        威尔逊得分计算函数
        :param pos: 正例数
        :param total: 总数
        :param p_z: 正太分布的分位数，一般而言，样本数的量级越大，z的取值大
        :return: 威尔逊得分
        """
        if total == 0:      # 没有曝光
            return 0.00

        pos_rat = pos * 1.0 / total * 1.0  # 正例比率
        a = pos_rat + (np.square(p_z) / (2.0 * total))
        b = (p_z / (2.0 * total)) * np.sqrt(4.0 * total * (1.0 - pos_rat) * pos_rat + np.square(p_z))
        c = (1.0 + np.square(p_z) / total)
        return round((a - b) / c, 6)

    def run(self):
        pass


if __name__ == '__main__':
    pass
