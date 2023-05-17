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

file_dir = "Weekly"
all_csv_list = os.listdir(file_dir)
for single_csv in all_csv_list:
    single_data_frame = pd.read_csv(os.path.join(file_dir, single_csv))
    #     print(single_data_frame.info())
    if single_csv == all_csv_list[0]:
        all_data_frame = single_data_frame
        all_data_frame = all_data_frame[["日期", "Car_ID", "车辆在线时长（H）", "车辆在线率（百分比）", "接管次数","运营里程（KM）", "订单里程（KM）"]]
        all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)
    else:  # concatenate all csv to a single dataframe, ingore index
        all_data_frame = pd.concat([all_data_frame, single_data_frame], ignore_index=True)
        all_data_frame = all_data_frame[["日期", "Car_ID", "车辆在线时长（H）", "车辆在线率（百分比）", "接管次数","运营里程（KM）", "订单里程（KM）"]]
        all_data_frame = all_data_frame.sort_values(by=['日期'], ascending=True)
            
    sql = """
    SELECT Car_ID, ROUND(AVG(车辆在线时长（H）),2) AS 车辆在线时长（H）, ROUND(AVG(车辆在线率（百分比）), 2) AS 车辆在线率（百分比）, ROUND(AVG(接管次数),2) AS 接管次数, ROUND(AVG(运营里程（KM）),2) AS 运营里程（KM）, ROUND(AVG(订单里程（KM）),2) AS 订单里程（KM）
    FROM all_data_frame
    GROUP BY Car_ID
    ORDER BY length(Car_ID), Car_ID ASC
    """
    sql_result = ps.sqldf(sql, locals())
    print(sql_result)
    sql_result.to_excel('excel_output.xls')