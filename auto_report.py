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

class AutoReport(object):
    def __init__(self):
        self.dpa = DrPlatformAPI()
        self.ja = JiraAPI()
        self.dca = DrControlAPI()
    
    """
    一、车辆运行情况
    Part. 1 运营车辆
    """
    def part_1a(self, date: pd.Timestamp = pd.Timestamp.today()):
        '''
        运营数据（日）
        '''
        # today = date.floor('d')
        # start_time = today - pd.Timedelta("1 days")
        # end_time = today - pd.Timedelta("0 days")
        
        start_time = date.floor('d')
        end_time = start_time + pd.Timedelta("1 days")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'
        
        # 判断是否为工作日
        now = datetime.datetime.today()
        today = now - datetime.timedelta(days = 0)
        yesterday = now - datetime.timedelta(days = 1)
        boll = is_workday(yesterday)


        data_frame = self.dpa.load_trips()
        
        sql_platform = """
        SELECT * 
        FROM
        (
        SELECT vehicleId AS Car_ID,location AS 城市 ,DATE(startTime)AS 日期, SUM(takeoverTimes) AS 接管次数 , SUM(distance) AS 运营里程（KM）
        FROM data_frame
        WHERE taskType = "运营"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        GROUP BY vehicleId
        )
        ORDER BY length(Car_ID), Car_ID ASC
        """

        sql_platform_result = ps.sqldf(sql_platform, locals())
        sql_platform_result = sql_platform_result[sql_platform_result['Car_ID'].str.contains('YR_MKZ_2|YR_MKZ_3|YR_MKZ_5|YR_MKZ_6|YR_MKZ_9|YR_MKZ_10|YR_MKZ_11|YR_MKZ_12|YR_ALX_1|YR_ALX_3|YR_ALX_4|YR_ALX_5|YR_ALX_6|YR_ALX_7|YR_ALX_10|YR_MAR_15|YR_MAR_19|YR_MAR_30')]
        
        # 运营时间：工作日6小时
        if boll == True: 
            '''
            添加dtu的运营订单数据
            '''
            try:
                dtu_data = self.dca.count_operation_data()
                # dtu_data = dtu_data.iloc[:,:2]
                dtu_data = dtu_data[['index', '乘客乘车距离','车辆在线时长', '完成订单数']]
                # dtu_data = dtu_data.columns = ['Car_ID','订单里程（KM）']
                dtu_data = dtu_data.rename(columns={'index':'Car_ID'})
                dtu_data = dtu_data.rename(columns={'乘客乘车距离':'订单里程（KM）'})
                dtu_data = dtu_data.rename(columns={'车辆在线时长':'车辆在线时长（H）'})
                
        
                merge_data = sql_platform_result.merge(dtu_data, on = 'Car_ID', how = 'left')
                merge_data= merge_data.round({'订单里程（KM）': 2})
                merge_data= merge_data.round({'车辆在线时长（H）': 2})
                merge_data['订单里程（KM）'] = merge_data['订单里程（KM）'].astype(str).str.replace("nan", "0")
                merge_data['车辆在线时长（H）'] = merge_data['车辆在线时长（H）'].fillna(value=0)
                merge_data['完成订单数'] = merge_data['完成订单数'].fillna(value=0)
                
            
                sql_merge_data = """
                SELECT Car_ID, 城市, 日期, 车辆在线时长（H）, ROUND(车辆在线时长（H）/6 * 100, 2) AS 车辆上线率（百分比）, 接管次数, 运营里程（KM）,订单里程（KM）,完成订单数
                FROM merge_data
                ORDER BY length(Car_ID), Car_ID ASC
                """
                sql_merge_data_result = ps.sqldf(sql_merge_data, locals())
                return sql_merge_data_result
            except:
                # print("error")
                return sql_platform_result

        # 运营时间：周末6.5小时
        if boll == False:
            '''
            添加dtu的运营订单数据
            '''
            try:
                dtu_data = self.dca.count_operation_data()
                # dtu_data = dtu_data.iloc[:,:2]
                dtu_data = dtu_data[['index', '乘客乘车距离','车辆在线时长', '完成订单数']]
                # dtu_data = dtu_data.columns = ['Car_ID','订单里程（KM）']
                dtu_data = dtu_data.rename(columns={'index':'Car_ID'})
                dtu_data = dtu_data.rename(columns={'乘客乘车距离':'订单里程（KM）'})
                dtu_data = dtu_data.rename(columns={'车辆在线时长':'车辆在线时长（H）'})
        
                merge_data = sql_platform_result.merge(dtu_data, on = 'Car_ID', how = 'left')
                merge_data= merge_data.round({'订单里程（KM）': 2})
                merge_data= merge_data.round({'车辆在线时长（H）': 2})
                merge_data['订单里程（KM）'] = merge_data['订单里程（KM）'].astype(str).str.replace("nan", "0")
                merge_data['车辆在线时长（H）'] = merge_data['车辆在线时长（H）'].fillna(value=0)
                merge_data['完成订单数'] = merge_data['完成订单数'].fillna(value=0)
            
                sql_merge_data = """
                SELECT Car_ID, 城市, 日期, 车辆在线时长（H）, ROUND(车辆在线时长（H）/6.5 * 100, 2) AS 车辆上线率（百分比）, 接管次数, 运营里程（KM）,订单里程（KM）,完成订单数
                FROM merge_data
                ORDER BY length(Car_ID), Car_ID ASC
                """
                sql_merge_data_result = ps.sqldf(sql_merge_data, locals())
        
                return sql_merge_data_result
            except:
                return sql_platform_result


    def part_1b(self):
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
        result = result[result['Car_ID'].str.contains('YR_MKZ_2|YR_MKZ_3|YR_MKZ_5|YR_MKZ_6|YR_MKZ_9|YR_MKZ_10|YR_MKZ_11|YR_MKZ_12|YR_ALX_1|YR_ALX_3|YR_ALX_4|YR_ALX_5|YR_ALX_6|YR_ALX_7|YR_ALX_10|YR_MAR_15|YR_MAR_19|YR_MAR_30')]
        
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
                merge_data['订单里程（KM）'] = merge_data['订单里程（KM）'].astype(str).str.replace("nan", "None")
                merge_data['车辆在线时长（H）'] = merge_data['车辆在线时长（H）'].fillna(value=0)
                
                # 日期    Car_ID   接管次数  运营里程（KM） 订单里程（KM）  车辆在线时长（H） 
                sql_merge_data = """
                SELECT 日期, COUNT(DISTINCT Car_ID) AS 运营车辆总数, SUM(车辆在线时长（H）) AS 车辆总上线时间（H）, ROUND(SUM(车辆在线时长（H）)/COUNT(DISTINCT Car_ID), 2) AS 平均单车上线时间（H） , ROUND(SUM(车辆在线时长（H）)/COUNT(DISTINCT Car_ID)/6 * 100, 2) AS 平均在线率（百分比）, SUM(接管次数) AS 车辆总接管次数, SUM(运营里程（KM）) AS 运营总里程, SUM(订单里程（KM）) AS 订单总里程
                FROM merge_data
                GROUP BY 日期
                ORDER BY length(Car_ID), Car_ID ASC
                """
                sql_merge_data_result = ps.sqldf(sql_merge_data, locals())
                
                """
                把每日数据存入csv
                """
                now = datetime.datetime.today()
                yesterday = now - datetime.timedelta(days = 1)
                yesterday = yesterday.strftime("%Y-%m-%d")
                
                sql_merge_data_result.to_csv("file/"+ yesterday +".csv")

                file_dir = "file"
                all_csv_list = os.listdir(file_dir)
                # all_csv_list.sort(key=lambda fn:os.path.getmtime(all_csv_list+'\\'+fn))
                # seven_csv_list = os.path.join(file_dir, all_csv_list[-7])     

                for single_csv in all_csv_list:
                    single_data_frame = pd.read_csv(os.path.join(file_dir, single_csv))
                    #     print(single_data_frame.info())
                    if single_csv == all_csv_list[0]:
                        all_data_frame = single_data_frame
                        all_data_frame = all_data_frame[["日期", "运营车辆总数", "车辆总上线时间（H）", "平均单车上线时间（H）", "平均在线率（百分比）", "车辆总接管次数", "运营总里程", "订单总里程"]]
                        all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)
                    else:  # concatenate all csv to a single dataframe, ingore index
                        all_data_frame = pd.concat([all_data_frame, single_data_frame], ignore_index=True)
                        all_data_frame = all_data_frame[["日期", "运营车辆总数", "车辆总上线时间（H）", "平均单车上线时间（H）", "平均在线率（百分比）","车辆总接管次数", "运营总里程", "订单总里程"]]
                        all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)

                return all_data_frame
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
                SELECT 日期, COUNT(DISTINCT Car_ID) AS 运营车辆总数, SUM(车辆在线时长（H）) AS 车辆总上线时间（H）, ROUND(SUM(车辆在线时长（H）)/COUNT(DISTINCT Car_ID), 2) AS 平均单车上线时间（H） , ROUND(SUM(车辆在线时长（H）)/COUNT(DISTINCT Car_ID)/6.5 * 100, 2) AS 平均在线率（百分比）, SUM(接管次数) AS 车辆总接管次数, SUM(运营里程（KM）) AS 运营总里程, SUM(订单里程（KM）) AS 订单总里程
                FROM merge_data
                GROUP BY 日期
                ORDER BY length(Car_ID), Car_ID ASC
                """
                sql_merge_data_result = ps.sqldf(sql_merge_data, locals())
                
                """
                把每日数据存入csv
                """
                now = datetime.datetime.today()
                yesterday = now - datetime.timedelta(days = 1)
                yesterday = yesterday.strftime("%Y-%m-%d")
                
                sql_merge_data_result.to_csv("file/"+ yesterday +".csv")

                file_dir = "file"
                all_csv_list = os.listdir(file_dir)
                for single_csv in all_csv_list:
                    single_data_frame = pd.read_csv(os.path.join(file_dir, single_csv))
                    #     print(single_data_frame.info())
                    if single_csv == all_csv_list[0]:
                        all_data_frame = single_data_frame
                        all_data_frame = all_data_frame[["日期", "运营车辆总数", "车辆总上线时间（H）", "平均单车上线时间（H）", "平均在线率（百分比）", "车辆总接管次数", "运营总里程", "订单总里程"]]
                        all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)
                    else:  # concatenate all csv to a single dataframe, ingore index
                        all_data_frame = pd.concat([all_data_frame, single_data_frame], ignore_index=True)
                        all_data_frame = all_data_frame[["日期", "运营车辆总数", "车辆总上线时间（H）", "平均单车上线时间（H）", "平均在线率（百分比）","车辆总接管次数", "运营总里程", "订单总里程"]]
                        all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)

                return all_data_frame
            except:
                return result
        
    # def part_1c(self, date: pd.Timestamp = pd.Timestamp.today()):
    #     """
    #     运营数据统计（日）折线图 Optional
    #     """
    #     now_time = date.floor('d')
    #     end_time = now_time + pd.Timedelta("1 days")
    #     start_time = end_time - pd.Timedelta("3 days")

    #     start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    #     end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    #     start_time = f'"{start_time}"'
    #     end_time = f'"{end_time}"'

    #     data_frame = self.dpa.load_trips()
    #     sql = """
    #             SELECT T.日期, COUNT(DISTINCT T.vehicleId)AS 车辆总数, SUM(T.使用时间（h）)AS 总时间（H）, ROUND(((SUM(T.使用时间（h）))/(COUNT(DISTINCT T.vehicleId))),2)AS 平均运营时间（H）
    #             FROM
    #             (
    #             SELECT DATE(startTime)AS 日期, vehicleId, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2) AS 使用时间（h）
    #             FROM data_frame
    #             WHERE taskType = "运营"
    #             AND startTime > """ + start_time + """
    #             AND startTime < """ + end_time + """
    #             ) T
    #             GROUP BY T.日期
    #             ORDER BY T.日期
    #     """
    #     result = ps.sqldf(sql, locals())
    #     df = result
    #     fig = go.Figure()
    #     # Create and style traces
    #     fig.add_trace(go.Scatter(x=df['日期'], y=df['车辆总数'], name='车辆总数',
    #                      line=dict(color='firebrick', width=4)))
    #     fig.add_trace(go.Scatter(x=df['日期'], y=df['总时间（H）'], name = '总时间（H）',
    #                      line=dict(color='royalblue', width=4)))
    #     fig.add_trace(go.Scatter(x=df['日期'], y=df['平均运营时间（H）'], name='平均运营时间（H）',
    #                      line=dict(color='orange', width=4) # dash options include 'dash', 'dot', and 'dashdot'
    #     ))

    #     # Edit the layout
    #     fig.update_layout(title={
    #         'text' : '运营数据统计',
    #         'x':0.5,
    #         'xanchor': 'center'
    #     },
    #         xaxis_title='日期',
    #                #yaxis_title='xxx'
    #             )


    #     # fig.show()
        

    """
    Part. 2 测试车辆
    测试任务情况、其他任务情况、车辆空闲情况
    """
    def part_2a(self, date: pd.Timestamp = pd.Timestamp.today()):
        """
        测试数据（日）
        """
        
        # today = date.floor('d')
        # start_time = today - pd.Timedelta("1 days")
        # end_time = today - pd.Timedelta("0 days")
        start_time = date.floor('d')
        end_time = start_time + pd.Timedelta("1 days")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'

        data_frame = self.dpa.load_trips()
        stg_dataframe = self.dpa.load_stg_trips()
        data_frame_all = pd.concat( [data_frame, stg_dataframe], axis=0)
        
        sql1 = """
        SELECT S.Task_Id, S.日期, S.Task_Type, COUNT(DISTINCT S.Car_ID) AS 数量, ROUND(SUM(S.实际测试时长（H）),2) AS 实际测试时长（H）, ROUND(SUM(S.总里程（KM）),2) AS 总里程（KM）
        FROM
        (
        SELECT vehicleId AS Car_ID, CAST(taskId AS INT) AS Task_Id, taskType AS Task_Type, DATE(startTime)AS 日期,ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 实际测试时长（H）, distance AS 总里程（KM） 
        FROM data_frame_all
        WHERE taskType LIKE "%测试"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        ) S
        GROUP BY S.Task_Id
        """
        result1 = ps.sqldf(sql1, locals())
        
        
        sql2 = """
        SELECT DISTINCT S.Car_ID, S.Task_Id
        FROM
        (
        SELECT vehicleId AS Car_ID,CAST(taskId AS INT) AS Task_Id, DATE(startTime)AS 日期,ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 实际测试时长（H）, distance AS 总里程（KM） 
        FROM data_frame_all
        WHERE taskType LIKE "%测试"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        ) S
        """
        result2 = ps.sqldf(sql2, locals())
        
        merge_table = pd.merge(result1, result2, on = 'Task_Id')
        new_table = merge_table.groupby(['Task_Id','日期','数量','实际测试时长（H）','Task_Type', '总里程（KM）'])['Car_ID'].sum()

        """
        关联OT，用新的dataframe
        """
        task_data_frame = self.dpa.load_tasks()
        sql3 = """
        SELECT id AS Task_Id, jiraIssues AS OT
        FROM task_data_frame
        WHERE taskType LIKE "%测试"
        """
        result3 = ps.sqldf(sql3, locals())
        # result3.to_excel("output.xlsx")
        
        temp = pd.merge(new_table, result3, how='left', on='Task_Id')
        #print(temp)
        
        sql4 = """
        SELECT new_table.Task_Id, new_table.Task_Type, new_table.日期, new_table.数量, new_table.实际测试时长（H）, new_table.总里程（KM）, new_table.Car_ID, temp.OT
        FROM new_table
        LEFT JOIN temp
        WHERE temp.Task_Id = new_table.Task_Id
        """
        result4 = ps.sqldf(sql4, locals())
        order = ['Task_Type', 'OT','Task_Id','日期','数量', 'Car_ID' ,'实际测试时长（H）','总里程（KM）']
        result4 = result4[order]
        # result4 = result4[result4['Car_ID'].str.contains('YR_MKZ_1|YR_MKZ_7|YR_MKZ_8|YR_ALX_2|YR_ALX_9|YR_MAR_13|YR_MAR_24')]
        result4 = result4[result4['Task_Type'].str.contains('测试|模块测试|集成测试')]
        return result4
    
    # def part_2b(self, date: pd.Timestamp = pd.Timestamp.today()):
    #     """
    #     测试数据统计（日）
    #     """
    #     now_time = date.floor('d')
    #     end_time = now_time + pd.Timedelta("1 days")
    #     start_time = end_time - pd.Timedelta("3 days")

    #     start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    #     end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    #     start_time = f'"{start_time}"'
    #     end_time = f'"{end_time}"'

    #     data_frame = self.dpa.load_trips()
    #     stg_dataframe = self.dpa.load_stg_trips()
    #     data_frame_all = pd.concat( [data_frame, stg_dataframe], axis=0)
        
    #     sql = """
    #     SELECT T.日期, COUNT(DISTINCT T.vehicleId)AS 车辆总数, SUM(T.使用时间（H）)AS 总时间（H）, ROUND(((SUM(T.使用时间（H）))/(COUNT(DISTINCT T.vehicleId))),2)AS 平均测试时间（H）
    #     FROM
    #     (
    #     SELECT DATE(startTime)AS 日期, vehicleId, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2) AS 使用时间（H）
    #     FROM data_frame_all
    #     WHERE taskType = "测试"
    #     AND startTime > """ + start_time + """
    #     AND startTime < """ + end_time + """
    #     ) T
    #     GROUP BY T.日期
    #     """
    #     result = ps.sqldf(sql, locals())
    #     return result
    
    # def part_2c(self, date: pd.Timestamp = pd.Timestamp.today()):
    #     """
    #     测试数据统计（日）折线图
    #     """
    #     now_time = date.floor('d')
    #     end_time = now_time + pd.Timedelta("1 days")
    #     start_time = end_time - pd.Timedelta("3 days")

    #     start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    #     end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    #     start_time = f'"{start_time}"'
    #     end_time = f'"{end_time}"'

    #     data_frame = self.dpa.load_trips()
    #     stg_dataframe = self.dpa.load_stg_trips()
    #     data_frame_all = pd.concat( [data_frame, stg_dataframe], axis=0)
        
    #     sql = """
    #     SELECT T.日期, COUNT(DISTINCT T.vehicleId)AS 车辆总数, SUM(T.使用时间（H）)AS 总时间（H）, ROUND(((SUM(T.使用时间（H）))/(COUNT(DISTINCT T.vehicleId))),2)AS 平均测试时间（H）
    #     FROM
    #     (
    #     SELECT DATE(startTime)AS 日期, vehicleId, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2) AS 使用时间（H）
    #     FROM data_frame_all
    #     WHERE taskType = "测试"
    #     AND startTime > """ + start_time + """
    #     AND startTime < """ + end_time + """
    #     ) T
    #     GROUP BY T.日期
    #     ORDER BY T.日期
    #     """
    #     result = ps.sqldf(sql, locals())
    #     df = result
    #     fig = go.Figure()
    #     # Create and style traces
    #     fig.add_trace(go.Scatter(x=df['日期'], y=df['车辆总数'], name='车辆总数',
    #                      line=dict(color='firebrick', width=4)))
    #     fig.add_trace(go.Scatter(x=df['日期'], y=df['总时间（H）'], name = '总时间（H）',
    #                      line=dict(color='royalblue', width=4)))
    #     fig.add_trace(go.Scatter(x=df['日期'], y=df['平均测试时间（H）'], name='平均测试时间（H）',
    #                      line=dict(color='orange', width=4) # dash options include 'dash', 'dot', and 'dashdot'
    #     ))

    #     # Edit the layout
    #     fig.update_layout(title={
    #         'text' : '测试数据统计',
    #         'x':0.5,
    #         'xanchor': 'center'
    #     },
    #         xaxis_title='日期',
    #         #yaxis_title='xxx'
    #              )

    #     # fig.show()
    
    def part_2b(self, date: pd.Timestamp = pd.Timestamp.today()):
        '''
        其他任务情况
        包括维护采集数据、采地图、安全员培训等
        '''
        
        # today = date.floor('d')
        # start_time = today - pd.Timedelta("1 days")
        # end_time = today - pd.Timedelta("0 days")
        start_time = date.floor('d')
        end_time = start_time + pd.Timedelta("1 days")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'

        data_frame = self.dpa.load_trips()
        stg_dataframe = self.dpa.load_stg_trips()
        data_frame_all = pd.concat( [data_frame, stg_dataframe], axis=0)
        
        sql1 = """
        SELECT S.Task_Id, S.日期, S.taskType AS Task_Type, COUNT(DISTINCT S.Car_ID) AS 数量, ROUND(SUM(S.总时间（H）),2) AS 总时间（H）
        FROM
        (
        SELECT vehicleId AS Car_ID,CAST(taskId AS INT) AS Task_Id, taskType, DATE(startTime)AS 日期,ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 总时间（H） 
        FROM data_frame_all
        WHERE taskType = "建图"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        UNION ALL
        SELECT vehicleId AS Car_ID,CAST(taskId AS INT) AS Task_Id, taskType, DATE(startTime)AS 日期,ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 总时间（H）
        FROM data_frame_all
        WHERE taskType = "培训"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        ) S
        GROUP BY S.Task_Id
        """
        result1 = ps.sqldf(sql1, locals())
        
        sql2 = """
        SELECT DISTINCT S.Car_ID, S.Task_Id
        FROM
        (
        SELECT vehicleId AS Car_ID,CAST(taskId AS INT) AS Task_Id, DATE(startTime)AS 日期,ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 总时间（H）
        FROM data_frame_all
        WHERE taskType = "建图"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        UNION ALL
        SELECT vehicleId AS Car_ID,CAST(taskId AS INT) AS Task_Id, DATE(startTime)AS 日期,ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 总时间（H） 
        FROM data_frame_all
        WHERE taskType = "培训"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        ) S
        """
        result2 = ps.sqldf(sql2, locals())
        
        merge_table = pd.merge(result1, result2, on = 'Task_Id')
        new_table = merge_table.groupby(['Task_Id','Task_Type', '日期','数量','总时间（H）'])['Car_ID'].sum()

        """
        关联OT，用新的dataframe
        """
        task_data_frame = self.dpa.load_tasks()
        sql3 = """
        SELECT id AS Task_Id, jiraIssues AS OT
        FROM task_data_frame
        WHERE taskType = "培训"
        """
        result3 = ps.sqldf(sql3, locals())
        # result3.to_excel("output.xlsx")
        
        temp = pd.merge(new_table, result3, how='left', on='Task_Id')
        #print(temp)
        
        sql4 = """
        SELECT new_table.Task_Id, new_table.日期, new_table.Task_Type, new_table.数量, new_table.总时间（H）, new_table.Car_ID, temp.OT
        FROM new_table
        LEFT JOIN temp
        WHERE temp.Task_Id = new_table.Task_Id
        """
        result4 = ps.sqldf(sql4, locals())
        #print(result4)
        order = ['日期','Task_Type','数量', 'Car_ID', 'Task_Id', 'OT', '总时间（H）']

        result4 = result4[order]
        result4['OT'] = result4['OT'].str.replace("", "None")
        return result4
        
    def part_2c(self, date: pd.Timestamp = pd.Timestamp.today()):
        """
        车辆空闲情况(日)
        """
        now_time = date.floor('d')
        end_time = now_time + pd.Timedelta("1 days")
        start_time = end_time - pd.Timedelta("3 days")

        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'

        data_frame = self.dpa.load_trips()
        stg_dataframe = self.dpa.load_stg_trips()
        data_frame_all = pd.concat( [data_frame, stg_dataframe], axis=0)
        
        sql = """
        SELECT T.日期, T.Car_ID, T.空闲时长（H）, ROUND((T.空闲时长（H）/6) * 100, 2) AS 闲置率（百分百）
        FROM
        (
        SELECT DATE(startTime)AS 日期, vehicleId AS Car_ID, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2) AS 空闲时长（H）
        FROM data_frame_all
        WHERE taskType = "测试"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        GROUP BY Car_ID
        ) T
        WHERE T.空闲时长（H） > 3
        ORDER BY length(T.Car_ID), T.Car_ID ASC
        """
        result = ps.sqldf(sql, locals())
        # result = result[result['Car_ID'].str.contains('YR_MKZ_1|YR_MKZ_7|YR_MKZ_8|YR_ALX_2|YR_ALX_9|YR_MAR_13|YR_MAR_24')]
        return result
    
    """
    Part. 3 Demo数据
    手动输出
    """
    
    """
    Part. 4 异常记录
    """
    def part_4a(self, date: pd.Timestamp = pd.Timestamp.today()):
        start_time = date.floor('d')
        end_time = start_time + pd.Timedelta("1 days")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'

        data_frame = self.dpa.load_trips()
        stg_dataframe = self.dpa.load_stg_trips()
        data_frame_all = pd.concat( [data_frame, stg_dataframe], axis=0)
        
        sql = """
        SELECT *
        FROM
        (
        SELECT CAST(taskId AS INT) AS Task_Id, taskType AS Task_Type, id AS Trip_ID, vehicleId AS Car_ID, location AS 城市, DATE(startTime)AS 日期, TIME(startTime) AS 开始时刻, TIME(endTime) AS 结束时刻, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 行程时间（H）, distance AS Trip里程（KM）, status AS 数据状态
        FROM data_frame_all
        WHERE startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        ) s 
        WHERE S.行程时间（H） < 0.08333
        ORDER BY S.开始时刻 ASC, length(S.Car_ID), S.Car_ID ASC
        """
        
        result = ps.sqldf(sql, locals())
        return result
    
    """
    OB Dashboard 
    """
    
    def part_5a(self):
        now = datetime.datetime.today()
        today = now - datetime.timedelta(days = 0) #0
        yesterday = now - datetime.timedelta(days = 1) #1
        end_time = today.strftime("%Y-%m-%d")
        start_time = yesterday.strftime("%Y-%m-%d")
        # print(start_time, end_time)
        try:
            jql = 'project = OB AND created >= ' + start_time + \
            ' AND created <= ' + end_time + ' AND "CAR ID" in (YR_MKZ_1, YR_MKZ_2, YR_MKZ_3, YR_MKZ_4, YR_MKZ_5,YR_MKZ_6, YR_MKZ_7, \
            YR_MKZ_8, YR_MKZ_9, YR_MKZ_10, YR_MKZ_11, YR_MKZ_12, YR_MKZ_13, YR_ALX_1, YR_ALX_2, YR_ALX_3, YR_ALX_4, YR_ALX_5, \
            YR_ALX_6, YR_ALX_7, YR_ALX_8, YR_ALX_9, YR_ALX_10, YR_MAR_13, YR_MAR_15, YR_MAR_24, YR_MAR_30) ORDER BY priority DESC, updated DESC'
            dataframe = self.ja.deal_data(jql)
            # dataframe['dataframe'] = dataframe['dataframe'].fillna(value = "未排查")
            # dataframe["components"] = dataframe["components"].str.replace("", "未排查")
            data = dataframe.groupby("components").count()
            data = data.reset_index()
            data.loc[0, 'components'] = "未排查"
            # print(data)
            fig = px.pie(values=data["key"], names=data["components"])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            # Edit the layout
            fig.update_layout(
                # title={
                # 'text' : '本日新增OB分布图',
                # 'x':0.5,
                # 'xanchor': 'center'}
                # width = 1000,
                height= 750
                )
            fig.show()
        except:
            pass
        # print(data)
        
    def part_5b(self):
        now = datetime.datetime.today()
        today = now - datetime.timedelta(days = 1) #1
        yesterday = now - datetime.timedelta(days = 2) #2
        end_time = today.strftime("%Y-%m-%d")
        start_time = yesterday.strftime("%Y-%m-%d")
        # print(start_time, end_time)
        try:
            jql = 'project = OB AND created >= ' + start_time + \
            ' AND created <= ' + end_time + ' AND "CAR ID" in (YR_MKZ_1, YR_MKZ_2, YR_MKZ_3, YR_MKZ_4, YR_MKZ_5,YR_MKZ_6, YR_MKZ_7, YR_MKZ_8, YR_MKZ_9, YR_MKZ_10, YR_MKZ_11, YR_MKZ_12, YR_MKZ_13, YR_ALX_1, YR_ALX_2, YR_ALX_3, YR_ALX_4, YR_ALX_5, YR_MKZ_3, YR_ALX_6, YR_ALX_7, YR_ALX_8, YR_ALX_9, YR_ALX_10, YR_MAR_13, YR_MAR_15, YR_MAR_24, YR_MAR_30)  AND assignee = empty ORDER BY priority DESC, updated DESC'
            return self.ja.deal_data(jql)
        except:
            pass

    
    """
    附件四表
    """
    def part_6a(self, date: pd.Timestamp = pd.Timestamp.today()):
        """
        运营数据详情
        """
        start_time = date.floor('d')
        end_time = start_time + pd.Timedelta("1 days")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'

        data_frame = self.dpa.load_trips()
        sql = """
        SELECT *
        FROM
        (
        SELECT CAST(taskId AS INT) AS Task_Id, taskType AS Task_Type, id AS Trip_ID, vehicleId AS Car_ID, location AS 城市, DATE(startTime)AS 日期,  TIME(startTime) AS 开始时刻, TIME(endTime) AS 结束时刻, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 行程时间（H）, distance AS Trip里程（KM）,status AS 数据状态 
        FROM data_frame
        WHERE taskType = "运营"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        )
        ORDER BY length(Car_ID), Car_ID ASC
        """
        result = ps.sqldf(sql, locals())
        result = result.replace("FrameDataReady", "已上传")
        result = result.replace("FrameDataParseFailed", "未上传")
        result = result.replace("FrameDataParsing", "未上传")
        result = result.replace("DataParseFailed", "未上传")
        result = result.replace("DataReady", "未上传")
        result = result.replace("DataParsing", "未上传")
        result = result.replace("DataUploaded", "未上传")
        result = result.replace("DataUploading", "未上传")
        result = result.replace("Started", "未上传")
        result = result.replace("End", "未上传")
        return result
    
    def part_6b(self, date: pd.Timestamp = pd.Timestamp.today()):
        """
        测试数据详情
        """
        start_time = date.floor('d')
        end_time = start_time + pd.Timedelta("1 days")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'
        
        data_frame = self.dpa.load_trips()
        stg_dataframe = self.dpa.load_stg_trips()
        data_frame_all = pd.concat( [data_frame, stg_dataframe], axis=0)
        
        sql = """
        SELECT *
        FROM 
        (
        SELECT CAST(taskId AS INT) AS Task_Id, taskType AS Task_Type, id AS Trip_ID, vehicleId AS Car_ID, location AS 城市, DATE(startTime)AS 日期, TIME(startTime) AS 开始时刻, TIME(endTime) AS 结束时刻, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 行程时间（H）, distance AS Trip里程（KM）,status AS 数据状态 
        FROM data_frame_all
        WHERE taskType LIKE "%测试"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        UNION ALL
        SELECT CAST(taskId AS INT) AS Task_Id, taskType AS Task_Type, id AS Trip_ID, vehicleId AS Car_ID, location AS 城市, DATE(startTime)AS 日期, TIME(startTime) AS 开始时刻, TIME(endTime) AS 结束时刻, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 行程时间（H）, distance AS Trip里程（KM）,status AS 数据状态
        FROM data_frame_all
        WHERE taskType = "建图"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        GROUP BY vehicleId
        UNION ALL
        SELECT CAST(taskId AS INT) AS Task_Id, taskType AS Task_Type, id AS Trip_ID, vehicleId AS Car_ID, location AS 城市, DATE(startTime)AS 日期, TIME(startTime) AS 开始时刻, TIME(endTime) AS 结束时刻, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 行程时间（H）, distance AS Trip里程（KM）,status AS 数据状态
        FROM data_frame_all
        WHERE taskType = "培训"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        GROUP BY vehicleId
        )
        ORDER BY length(Car_ID), Car_ID ASC
        """

        result = ps.sqldf(sql, locals())
        result = result.replace("FrameDataReady", "已上传")
        result = result.replace("FrameDataParseFailed", "未上传")
        result = result.replace("FrameDataParsing", "未上传")
        result = result.replace("DataParseFailed", "未上传")
        result = result.replace("DataReady", "未上传")
        result = result.replace("DataParsing", "未上传")
        result = result.replace("DataUploading", "未上传")
        result = result.replace("DataUploaded", "未上传")
        result = result.replace("Started", "未上传")
        result = result.replace("End", "未上传")
        result = result[result['Task_Type'].str.contains('测试|模块测试|集成测试')]
        return result

    def part_6c(self, date: pd.Timestamp = pd.Timestamp.today()):
        """
        Demo数据详情
        """
        start_time = date.floor('d')
        end_time = start_time + pd.Timedelta("1 days")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        start_time = f'"{start_time}"'
        end_time = f'"{end_time}"'

        data_frame = self.dpa.load_trips()
        sql = """
        SELECT *
        FROM
        (
        SELECT CAST(taskId AS INT) AS Task_Id, taskType AS Task_Type, id AS Trip_ID, vehicleId AS Car_ID, location AS 城市, DATE(startTime)AS 日期,  TIME(startTime) AS 开始时刻, TIME(endTime) AS 结束时刻, ROUND((JULIANDAY(endTime) - JULIANDAY(startTime)) * 24,2)AS 行程时间（H）, distance AS Trip里程（KM）,status AS 数据状态 
        FROM data_frame
        WHERE taskType = "Demo"
        AND startTime > """ + start_time + """
        AND startTime < """ + end_time + """
        )
        ORDER BY length(Car_ID), Car_ID ASC
        """
        result = ps.sqldf(sql, locals())
        result = result.replace("FrameDataReady", "已上传")
        result = result.replace("FrameDataParseFailed", "未上传")
        result = result.replace("FrameDataParsing", "未上传")
        result = result.replace("DataParseFailed", "未上传")
        result = result.replace("DataReady", "未上传")
        result = result.replace("DataParsing", "未上传")
        result = result.replace("DataUploaded", "未上传")
        result = result.replace("DataUploading", "未上传")
        result = result.replace("Started", "未上传")
        result = result.replace("End", "未上传")
        return result

    def part_6d(self):
        """
        OB新增问题详情
        """
        now = datetime.datetime.today()
        today = now - datetime.timedelta(days = 0)
        yesterday = now - datetime.timedelta(days = 1)
        end_time = today.strftime("%Y-%m-%d")
        start_time = yesterday.strftime("%Y-%m-%d")
        # print(start_time, end_time)
        try:
            jql = 'project = OB AND created >= ' + start_time + \
            ' AND created <= ' + end_time + ' AND "CAR ID" in (YR_MKZ_1, YR_MKZ_2, YR_MKZ_3, YR_MKZ_4, YR_MKZ_5,YR_MKZ_6, YR_MKZ_7, \
            YR_MKZ_8, YR_MKZ_9, YR_MKZ_10, YR_MKZ_11, YR_MKZ_12, YR_MKZ_13, YR_ALX_1, YR_ALX_2, YR_ALX_3, YR_ALX_4, YR_ALX_5, \
            YR_ALX_6, YR_ALX_7, YR_ALX_8, YR_ALX_9, YR_ALX_10, YR_MAR_13, YR_MAR_14, YR_MAR_15, YR_MAR_24, YR_MAR_30) ORDER BY priority DESC, updated DESC'
            # jql = 'project = OB AND created >= 2021-12-11 AND created <= 2021-12-12 ORDER BY priority DESC, updated DESC'
            return self.ja.deal_data(jql)
        except:
            pass
        
    
if __name__ == '__main__':
    ar = AutoReport()
    ar.part_1a()
    ar.part_1b()
    # ar.part_1c()
    ar.part_2a()
    ar.part_2b()
    ar.part_2c()
    ar.part_4a()
    ar.part_5a()
    ar.part_5b()
    ar.part_6a()
    ar.part_6b()
    ar.part_6c()
    ar.part_6d()
