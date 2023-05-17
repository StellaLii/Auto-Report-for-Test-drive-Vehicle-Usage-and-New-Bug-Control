#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json
import pandas as pd
from pandas import DataFrame
import pandasql as ps
pd.options.mode.chained_assignment = None


class DrPlatformAPI(object):
    '''
    Drplatform API doc: 
        http://10.10.10.66/dr-pipeline/doc.html#/home
        http://10.10.10.66/task-service/doc.html#/home
    '''
    def __init__(self):
        pass

    def authority(self):
        pass

    def load_trips(self):
        '''
        TripSearchGetRequest{
        description	string
        endTime	integer($int64)
        id	integer($int32)
        locations	[string]
        name	string
        orderBy	string
        rangeConditions	[RangeSearchCondition{...}]
        securityOfficers	[string]
        startTime	integer($int64)
        statuses	[string]
        taskId	integer($int32)
        taskTypes	[string]
        testers	[string]
        tripTags	[string]
        vehicleIds	[string]
        }
        '''
        url = "http://data.internal.deeproute.cn/dr-pipeline/trip/query"
        trips_param = json.dumps({
            "condition": {
                "vehicle_id": {
                    "eq": [
                        'YR_MKZ_1', 'YR_MKZ_2', 'YR_MKZ_3', 'YR_MKZ_4', 'YR_MKZ_5', 'YR_MKZ_6',
                        'YR_MKZ_7', 'YR_MKZ_8', 'YR_MKZ_9', 'YR_MKZ_10', 'YR_MKZ_11',
                        'YR_MKZ_12', 'YR_MKZ_13', 'YR_ALX_1', 'YR_ALX_2', 'YR_ALX_3', 'YR_ALX_4', 
                        'YR_ALX_5', 'YR_ALX_6', 'YR_ALX_7', 'YR_ALX_8', 'YR_ALX_9', 'YR_ALX_10', 'YR_MAR_13', 'YR_MAR_14',
                        'YR_MAR_15', 'YR_MAR_19', 'YR_MAR_30', 'YR_MAR_24'
                        ]
                    }
                },
            "page": 0,
            "size": 1000,
            "orderBys": []
            })
        
        headers = {
            'Authorization': '{{Authorization}}',
            'Content-Type': 'application/json'
            }
        response = requests.request("POST", url, headers=headers, data=trips_param)
       
        #Data Manipulation
        response_data = response.json()
        new_response = response_data["body"]

        data_frame=pd.DataFrame(new_response).fillna('null')
        
        timestamp_cols = ['startTime', 'endTime', 'createdTime']
        convert_func = lambda x: pd.to_datetime(x, unit='ms', errors='coerce'
                                                ) + pd.Timedelta('08:00:00')
        for col in timestamp_cols:
            data_frame[col] = data_frame[col].apply(convert_func)

        data_frame = data_frame[["id","taskId","vehicleId","location","taskType","startTime","endTime","taskType","distance", "securityOfficer","tester","status", "takeoverTimes"]]
        # print(data_frame)
        return data_frame

    """
    Add stg数据 测试环境
    https://stg-drplatform.deeproute.cn/dr-pipeline/trip/query
    """
    def load_stg_trips(self):
        '''
        TripSearchGetRequest{
        description	string
        endTime	integer($int64)
        id	integer($int32)
        locations	[string]
        name	string
        orderBy	string
        rangeConditions	[RangeSearchCondition{...}]
        securityOfficers	[string]
        startTime	integer($int64)
        statuses	[string]
        taskId	integer($int32)
        taskTypes	[string]
        testers	[string]
        tripTags	[string]
        vehicleIds	[string]
        }
        '''
        url = "https://stg-drplatform.deeproute.cn/dr-pipeline/trip/query"
        trips_param = json.dumps({
            "condition": {
                "vehicle_id": {
                    "eq": [
                        'YR_MKZ_1', 'YR_MKZ_2', 'YR_MKZ_3', 'YR_MKZ_4', 'YR_MKZ_5', 'YR_MKZ_6',
                        'YR_MKZ_7', 'YR_MKZ_8', 'YR_MKZ_9', 'YR_MKZ_10', 'YR_MKZ_11',
                        'YR_MKZ_12', 'YR_MKZ_13', 'YR_ALX_1', 'YR_ALX_2', 'YR_ALX_3', 'YR_ALX_4', 
                        'YR_ALX_5', 'YR_ALX_6', 'YR_ALX_7', 'YR_ALX_8', 'YR_ALX_9', 'YR_ALX_10','YR_MAR_13', 'YR_MAR_14',
                        'YR_MAR_15', 'YR_MAR_19', 'YR_MAR_30', 'YR_MAR_24'
                        ]
                    }
                },
            "page": 0,
            "size": 1000,
            "orderBys": []
            })
        
        headers = {
            'Authorization': '{{Authorization}}',
            'Content-Type': 'application/json'
            }
        response = requests.request("POST", url, headers=headers, data=trips_param)
       
        #Data Manipulation
        response_data = response.json()
        new_response = response_data["body"]

        data_frame=pd.DataFrame(new_response).fillna('null')
        
        timestamp_cols = ['startTime', 'endTime', 'createdTime']
        convert_func = lambda x: pd.to_datetime(x, unit='ms', errors='coerce'
                                                ) + pd.Timedelta('08:00:00')
        for col in timestamp_cols:
            data_frame[col] = data_frame[col].apply(convert_func)

        stg_data_frame = data_frame[["id","taskId","vehicleId","location","taskType","startTime","endTime","taskType","distance", "securityOfficer","tester","status", "takeoverTimes"]]
        return stg_data_frame

    def load_tasks(self):
        url = "http://data.internal.deeproute.cn/dr-pipeline/task/query"
        
        payload = json.dumps({
            "condition": {
                "vehicle_ids": {
                    "contain": [

                    ]
                }
            },
            "page": 0,
            "size": 1000
            })
        headers = {
            'Content-Type': 'application/json'
            }
        response = requests.request("POST", url, headers=headers, data=payload)

        #Data Manipulation
        response_data = response.json()
        new_response = response_data["body"]

        df=pd.DataFrame(new_response).fillna('null')
        ls = df.values.tolist()
        ls.insert(0,df.columns.tolist())
        new_data = DataFrame(ls)

        new_data, new_data.columns = new_data[1:] , new_data.iloc[0]

        new_data["createdTime"] = pd.to_datetime(new_data["createdTime"], unit='ms',errors='coerce')+ pd.Timedelta('08:00:00')
        
        data_frame_task = new_data[["id","jiraIssues","createdTime","taskType"]]
        data_frame_task["jiraIssues"] = data_frame_task["jiraIssues"].astype(str)
        data_frame_task["jiraIssues"] = data_frame_task["jiraIssues"].str[15:22]
        # try:
        #     for i in data_frame_task["jiraIssues"]:
        #         OT = i[0]["issueKey"]
        #         print(OT)
        #         # data_frame_task.reset_index()
        #         # data_frame_task['OT'] = OT
        # except:
        #     pass

        return data_frame_task

    # def query_trips_by_time(self, start_time: pd.Timestamp,
    #                         end_time: pd.Timestamp):
    #     trips_param = {
    #         "condition": {
    #             "startTime": {"ge": start_time.timestamp() * 1000},
    #             "endTime": {"le": end_time.timestamp() * 1000},
    #             "vehicle_id": {
    #                 "eq": [
    #                     'YR_MKZ_1', 'YR_MKZ_2', 'YR_MKZ_3', 'YR_MKZ_4', 'YR_MKZ_5', 'YR_MKZ_6',
    #                     'YR_MKZ_7', 'YR_MKZ_8', 'YR_MKZ_9', 'YR_MKZ_10', 'YR_MKZ_11',
    #                     'YR_MKZ_12', 'YR_MKZ_13', 'YR_ALX_1', 'YR_ALX_2', 'YR_ALX_3', 'YR_ALX_4', 
    #                     'YR_ALX_5', 'YR_ALX_6', 'YR_ALX_7', 'YR_ALX_8', 'YR_ALX_9', 'YR_ALX_10'
    #                     ]
    #                 }},
    #         "page": 0,
    #         "size": 400,
    #         "orderBys": []
    #         }
                                 
        # data_frame = self.load_trips(trips_param)
        # return data_frame


if __name__ == '__main__':
    dpa = DrPlatformAPI()
    r = dpa.load_trips()
    s = dpa.load_tasks()
    k = dpa.load_stg_trips()
    print(s)
