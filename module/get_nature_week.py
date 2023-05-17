'''
Descripttion: 
Version: 1.0
Author: easonhe
Date: 2021-12-03 17:56:04
LastEditors: easonhe
LastEditTime: 2021-12-16 13:46:33
'''

import pandas as pd
import time
import datetime
import os,sys
sys.path.append(os.path.realpath('.'))

class DealTime(object):

    def __init__(self):
        pass

    def get_day_begin(self, t, N=0):
        '''
        N为0时获取时间戳ts当天的起始时间戳，N为负数时前数N天，N为正数是后数N天
        24 时(小时)=86400 000 毫秒
        '''
        return int(time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime(t)), '%Y-%m-%d'))) + 86400 * N

    def get_week_begin(self, t, N=0, nat=2):
        '''
        N为0时获取时间戳ts当周的开始时间戳，N为负数时前数N周，N为整数是后数N周，此函数将周一作为周的第一天
        '''
        w = int(time.strftime('%w', time.localtime(t)))
        return self.get_day_begin(int(t - (w+nat)*86400)) + N*604800

    def get_week_time(self, n=0, nat =2):
        '''
        :param n: n为0时默认当前周，n为正数是后面周，n为负数为前一周
        :param nat:nat默认时，自然周为上周五到这周五，nat为0时，为本周一到下周一
        :return:
        '''
        t = time.time()
        natrul_bigin = time.strftime('%Y-%m-%d',time.localtime(self.get_week_begin(t,n, nat)))
        natrul_end = time.strftime('%Y-%m-%d',time.localtime(self.get_week_begin(t,n+1,nat)))
        return natrul_bigin, natrul_end
    def get_day_time(self, t=1):
        today = datetime.datetime.now()
        offset = datetime.timedelta(days=-t)
        re_date = (today+offset).strftime('%Y-%m-%d')
        today = today.strftime('%Y-%m-%d')
        return re_date, today



if __name__ == "__main__":
    c = DealTime()
    print(c.get_day_time())
