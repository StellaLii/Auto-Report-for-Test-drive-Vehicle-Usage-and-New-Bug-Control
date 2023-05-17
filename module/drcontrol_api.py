'''
Descripttion: 
Version: 1.0
Author: easonhe
Date: 2021-12-03 17:08:17
LastEditors: easonhe
LastEditTime: 2022-01-06 11:25:14
'''
#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import sys
from typing import ValuesView
sys.path.append(os.path.realpath('.'))
sys.path.append(os.path.realpath('..'))
from get_nature_week import DealTime
import requests
import pandas as pd
import numpy as np
import json
from os.path import join as pjoin
USER = '18098915212'
PASSWD = 'Deeproute123456'


class DrControlAPI(object):
    def __init__(self):
        self.token = self.authority(USER, PASSWD, 'ADMINER')
        self.dealtime = DealTime()
        self.pd = pd

    def authority(self, user, passwd, role):
        '''
        :param phone_number: 获取token账号
        :param passwd: 获取token密码
        :param role:登录角色
        :return: 返回登录账号token
        '''
        url = "https://operation.deeproute.cn:9443/drrun/account/LoginAccount"
        headers = {
            "Accept-Encoding": "gzip",
            "Connection": "keep-alive",
            "Content-Length": "0",
            "Referer":
            "https://servicewechat.com/wx403785d9d3724b1c/0/page-frame.html",
            "User-Agent":
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.7(0x18000730) NetType/WIFI Language/zh_CN",
            "consumer": "customer",
            "content-type": "application/json",
            "cur_role": role,
            "force": "true",
            "mac": "12345",
            "passwd": passwd,
            "phone_number": user,
        }
        response = requests.post(url, headers=headers, json={}).json()
        try:
            token = response["access_jwt"]
        except KeyError:
            print("access_jwt dose not exist!")
        return token
    
    def get_between_time(self,num = 0):
        '''
        处理查询时间
        :param num: 需要往前推多少天，默认返回昨天和当前时间，
        :return:
        '''
        last_day, now_day = self.dealtime.get_day_time(num)
        return last_day, now_day

    def format_day_time(self,time):
        '''
        处理接口请求是需要的时间格式
        :param time: 传入需要格式化时间
        :return: 返回一个处理后时间字典
        '''
        time1 = pd.Timestamp(time)
        result = {
            "year": time1.year,
            "month": time1.month,
            "day": time1.day,
        }
        return result

    def deal_car_date(self, file_path='common/drcontrol_common.json'):
        '''
        处理车辆列表、查询url
        :param file_path:
        :return: 返回车辆列表，环境url
        '''
        with open(file_path, 'r', encoding='utf-8') as f:
            file_json = json.load(f)
        car_list = file_json['car_list']
        url_dict = file_json['url_list']
        return car_list, url_dict

    def get_car_record(self,env_url,car_id,get_time):
        '''
        获取车辆订单信息
        :param env_url: 传入需要搜索的url
        :param car_id: 传入需要搜索的car_id
        :param get_time: 传入需要搜索的时间
        :return: 返回车辆订单信息字典
        '''
        url = env_url + "/drrun/analysis/GetEUTotalDemands"
        get_record_time = self.format_day_time(get_time)
        # print(get_record_time)
        headers = {
            "Connection": "keep-alive",
            "access_jwt": self.token,
            "content-type": "application/json",
        }
        get_data = {
            "car_id": car_id,
            "start_time": get_record_time,
            "end_time": get_record_time
        }
        response = requests.post(url, headers=headers, json=get_data).json()
        # print(response)
        if response['error_code']['error_code'] != 0:  # 判断是否请求失败
            if response['error_code']['error_msg'] == 'INTERNAL_ERROR':  # 判断返回错误码是否为网络错误
                print(car_id, response)
            return None
        records = response['eu_demands']
        return records
        
    def get_day_order_records(self,days=1):
        '''
        处理一天内订单数据
        :param days: 当前天数时间，默认为昨天数据
        :return: 返回当天订单数据列表
        '''
        last_day, now_day = self.get_between_time(days)
        self.deal_with_days(last_day)
        car_list, url_list = self.deal_car_date()
        all_dict = []
        for key,value in url_list.items():
            for j in car_list:
                op_dict = []
                # print(j)
                test = self.get_car_record(value, j, last_day)
                op_dict.append(j)
                op_dict.append(test)
                # op_dict[j] = test
                all_dict.append(op_dict)
        return dict(all_dict)
    
    def get_effect_data(self, num=1):
        '''
        筛选有用数据
        :param num:往前推多少天数据，默认为昨天年数据
        :return: 返回有用数据字典
        '''
        operation_data = self.get_day_order_records(num)
        effect_data = []
        for key,value in operation_data.items():
            tmp_list = []
            if value != None:
                if (value["go_destination_length"] != 0) or (value["go_pickup_length"] != 0) or (value["cruise_length"] != 0):
                    # print(key, value)
                    tmp_list.append(key)
                    tmp_list.append(value)
                    effect_data.append(tmp_list)
        
        return dict(effect_data)

    def count_operation_data(self, num=1):
        '''
        处理日报需要数据以及数据格式
        :param num: 往前推多少天数据，默认为昨天年数据
        :return:返回返回处理后数据的df
        '''
        operation_data = self.get_effect_data(num)
        operation_df = pd.DataFrame(operation_data).T
        operation_df = operation_df.fillna(0)
        # operation_df.values.round(2)
        
        # operation_df['guest_num'] = operation_df['guest_num'].astype('int')
        # operation_df['cruise_length'] = operation_df['cruise_length'].astype(
        #     'int')
        operation_df['go_destination_length'] = operation_df['go_destination_length'].astype(
            'int') / 1000
        # operation_df['go_pickup_length'] = operation_df['go_pickup_length'].astype(
        #     'int') / 1000
        # operation_df['cruise_length'] = operation_df['cruise_length'].astype(
        #     'int') / 1000
        # operation_df['other_length'] = operation_df['other_length'].astype(
        #     'int') / 1000
        # operation_df['demand_time_duration'] = operation_df['demand_time_duration'].astype(
        #     'int') / 3600
        operation_df['online_time_duration'] = operation_df['online_time_duration'].astype(
            'int') / 3600
        # operation_df.loc['total'] = operation_df.apply(
        #     lambda x: x.sum())
        self.deal_with_daily(operation_df[-1:])  # 处理运营日报数据临时存放
        self.deal_with_car_id(operation_df)  # 处理运营日报数据临时存放
        new_col = ['乘客乘车距离', '车辆到上车点', '巡游里程', '其他上线里程',
                   '完成订单数', '未完成订单数', '乘客总数', '订单时长', '车辆在线时长','巡游时长']
        operation_df.columns = new_col
        operation_df = operation_df.reset_index()
        # print(operation_df)
        return(operation_df)

    def deal_with_daily(self, data, file_path="old_data/operation_daily.json"):
        '''
        处理运营日报并写到json文件
        :param data:
        :param file_path:
        :return:
        '''
        data.to_json(file_path)

    def deal_with_car_id(self, data, file_path="old_data/operation_car_id.json"):
        '''
        处理运营日报car_id
        :param data:
        :param file_path:
        :return:
        '''
        invalid_car_id = {}
        invalid_car_id_list = data.index.tolist()
        car_str = '('+",".join(invalid_car_id_list[:-1])+')'
        invalid_car_id["car_str"] = str(car_str)
        car_json = json.dumps(invalid_car_id)
        with open(file_path, "w") as f:
            f.write(car_json)
            f.close

    def deal_with_days(self, time, file_path="old_data/operation_days.json"):
        operation_day = {}
        operation_day["days"] = str(time)
        operation_days = json.dumps(operation_day)
        with open(file_path, "w") as f:
            f.write(operation_days)
            f.close



        

    def query_order_records(self, car_id, car_type, start_time, end_time):
        def format_time(time: pd.Timestamp):
            result = {
                "year": time.year,
                "month": time.month,
                "day": time.day,
                "hour": time.hour,
                "minute": time.minute,
                "second": time.second
            }
            return result

        url = "https://drrun.deeproute.cn:9443/drrun/schedule/GetAnyUserOperationRecord"

        headers = {
            "Connection": "keep-alive",
            "access_jwt": self.token,
            "content-type": "application/json",
        }
        payload = {
            "car_id": car_id,
            "type": car_type,
            "page": 0,
            "page_size": 50,
            "event_type": ["ET_ARRIVED", "ET_CHANGE_ADDR"],
            "start": format_time(start_time),
            "end": format_time(end_time)
        }
        response = requests.post(url, headers=headers, json=payload).json()
        if response['error_code']['error_code'] != 0:
            print(response['error_code']['error_code'])
            return None
        records = response['records']
        return records
     

if __name__ == '__main__':
    dca = DrControlAPI()
    # cd = dca.get_day_order_records(1)
    dca.get_effect_data()
    test = dca.count_operation_data()
    dca.deal_with_daily(test)
    print(test)
