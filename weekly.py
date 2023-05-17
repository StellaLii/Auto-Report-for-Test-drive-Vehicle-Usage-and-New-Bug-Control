#!/usr/bin/python3
# -*- coding: utf-8 -*-
from module.drplatform_api import DrPlatformAPI
from module.jira_api import *
from module.drcontrol_api import *
import plotly.graph_objects as go
import plotly.express as px
import datetime
import pandas as pd
import pandasql as ps
import numpy as np
from chinese_calendar import is_workday
import os

class AutoReport(object):
    def __init__(self):
        self.dpa = DrPlatformAPI()
        self.ja = JiraAPI()
        self.dca = DrControlAPI()
        
    def weekly(self):
        """
        运营数据统计（近7日数据）
        需要提前储存
        """
        now = datetime.datetime.today()
        today = now - datetime.timedelta(days = 0)
        yesterday = now - datetime.timedelta(days = 1)
        end_time = today.strftime("%Y-%m-%d 00:00:00")
        start_time = yesterday.strftime("%Y-%m-%d 00:00:00")

        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'

        data_frame = self.dpa.load_trips()
        sql = """
        SELECT DATE(startTime)AS 日期, vehicleId AS Car_ID, SUM(takeoverTimes) AS 接管次数 , SUM(distance) AS 运营里程（KM）
        FROM data_frame
        WHERE taskType = "运营"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        GROUP BY Car_ID
        """ 
        result = ps.sqldf(sql, locals())
        result = result[result['Car_ID'].str.contains('YR_MKZ_2|YR_MKZ_6|YR_MKZ_9|YR_MKZ_10|YR_MKZ_11|YR_MKZ_12|YR_ALX_1|YR_ALX_3|YR_ALX_4|YR_ALX_5|YR_ALX_6|YR_ALX_7|YR_ALX_10')]
        
        # 判断是否为工作日
        now = datetime.datetime.today()
        today = now - datetime.timedelta(days = 0)
        yesterday = now - datetime.timedelta(days = 1)
        boll = is_workday(yesterday)

        if boll == True: 
            try:
                dtu_data = self.dca.count_operation_data()
                dtu_data = dtu_data[['index', '乘客乘车距离','车辆在线时长']]
                dtu_data = dtu_data.rename(columns={'index':'Car_ID'})
                dtu_data = dtu_data.rename(columns={'乘客乘车距离':'订单里程（KM）'})
                dtu_data = dtu_data.rename(columns={'车辆在线时长':'车辆在线时长（H）'})
                    
                merge_data = result.merge(dtu_data, on = 'Car_ID', how = 'left')
                merge_data= merge_data.round({'订单里程（KM）': 2})
                merge_data= merge_data.round({'车辆在线时长（H）': 2})
                merge_data['订单里程（KM）'] = merge_data['订单里程（KM）'].astype(str).str.replace("nan", "0")
                merge_data['车辆在线时长（H）'] = merge_data['车辆在线时长（H）'].fillna(value=0)
                
                
                # 日期    Car_ID   接管次数  运营里程（KM） 订单里程（KM）  车辆在线时长（H） 
                sql_merge_data = """
                SELECT 日期, Car_ID, 车辆在线时长（H）, ROUND(车辆在线时长（H）/6 * 100, 2) AS 车辆在线率（百分比）, 接管次数, 运营里程（KM）, 订单里程（KM）
                FROM merge_data
                ORDER BY length(Car_ID), Car_ID ASC
                """
                sql_merge_data_result = ps.sqldf(sql_merge_data, locals())
                
                """
                把每日数据存入csv
                """
                now = datetime.datetime.today()
                yesterday = now - datetime.timedelta(days = 1)
                yesterday = yesterday.strftime("%Y-%m-%d")
                
                sql_merge_data_result.to_csv("Weekly/"+ yesterday +".csv")
            except:
                return result
            
        if boll == False: 
            try:
                dtu_data = self.dca.count_operation_data()
                dtu_data = dtu_data[['index', '乘客乘车距离','车辆在线时长']]
                dtu_data = dtu_data.rename(columns={'index':'Car_ID'})
                dtu_data = dtu_data.rename(columns={'乘客乘车距离':'订单里程（KM）'})
                dtu_data = dtu_data.rename(columns={'车辆在线时长':'车辆在线时长（H）'})
                    
                merge_data = result.merge(dtu_data, on = 'Car_ID', how = 'left')
                merge_data= merge_data.round({'订单里程（KM）': 2})
                merge_data= merge_data.round({'车辆在线时长（H）': 2})
                merge_data['订单里程（KM）'] = merge_data['订单里程（KM）'].astype(str).str.replace("nan", "None")
                merge_data['车辆在线时长（H）'] = merge_data['车辆在线时长（H）'].fillna(value=0)
                
                # 日期    Car_ID   接管次数  运营里程（KM） 订单里程（KM）  车辆在线时长（H） 
                sql_merge_data = """
                SELECT 日期, Car_ID, 车辆在线时长（H）, ROUND(车辆在线时长（H）/6.5 * 100, 2) AS 车辆在线率（百分比）, 接管次数, 运营里程（KM）, 订单里程（KM）
                FROM merge_data
                ORDER BY length(Car_ID), Car_ID ASC
                """
                sql_merge_data_result = ps.sqldf(sql_merge_data, locals())
                
                """
                把每日数据存入csv
                """
                now = datetime.datetime.today()
                yesterday = now - datetime.timedelta(days = 1)
                yesterday = yesterday.strftime("%Y-%m-%d")
                
                sql_merge_data_result.to_csv("Weekly/"+ yesterday +".csv")
            except:
                return result
    
    # def summary(self):
    #     file_dir = "Weekly"
    #     all_csv_list = os.listdir(file_dir)
        
    #     for single_csv in all_csv_list:
    #         single_data_frame = pd.read_csv(os.path.join(file_dir, single_csv))
    #         #     print(single_data_frame.info())
    #         if single_csv == all_csv_list[0]:
    #             all_data_frame = single_data_frame
    #             all_data_frame = all_data_frame[["日期", "Car_ID", "车辆在线时长（H）", "车辆在线率（百分比）", "接管次数","运营里程（KM）", "订单里程（KM）"]]
    #             all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)
    #         else:  # concatenate all csv to a single dataframe, ingore index
    #             all_data_frame = pd.concat([all_data_frame, single_data_frame], ignore_index=True)
    #             all_data_frame = all_data_frame[["日期", "Car_ID", "车辆在线时长（H）", "车辆在线率（百分比）", "接管次数","运营里程（KM）", "订单里程（KM）"]]
    #             all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)
            
    #     sql = """
    #     SELECT Car_ID, AVG(车辆在线时长（H）), AVG(车辆在线率（百分比）), AVG(接管次数), AVG(运营里程（KM）), AVG(订单里程（KM）)
    #     FROM all_data_frame
    #     GROUP BY Car_ID
    #     ORDER BY length(Car_ID), Car_ID ASC
    #     """
    #     sql_result = ps.sqldf(sql, locals())
    #     sql_result.to_excel('excel_output.xls')
            
            
if __name__ == '__main__':
    ar = AutoReport()
    print(ar.weekly())    