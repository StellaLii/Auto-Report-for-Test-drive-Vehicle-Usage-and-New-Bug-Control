'''
Descripttion: 
Version: 1.0
Author: easonhe
Date: 2021-12-31 17:09:07
LastEditors: easonhe
LastEditTime: 2021-12-31 18:04:19
'''
import os
import sys
sys.path.append(os.path.realpath('.'))
sys.path.append(os.path.realpath('..'))
import json
import jira
import plotly.graph_objs as go
from jira import JIRA
import pandas as pd
from module.jira_api import JiraAPI
from module.get_nature_week import DealTime
from collections import Counter

class SetJiraJql():
    def __init__(self):
        self.ja = JiraAPI()
        self.dealweek = DealTime()
        self.last_week_begin, self.last_week_end = DealTime().get_week_time()

    def operation_week_jira_jql(self, num=0):
        '''
        组装常用的jql
        :param num: 周数据需要考虑往前移动多少周，默认为当前周为0
        :return: 返回字典
        '''
        operation_week_jira_jql = {}
        last_week_begin, last_week_end = self.dealweek.get_week_time(num)
        OB_all_jql = "project = OB AND (component in (DTU, Visualizer, 元启行, 元启行后台) OR assignee in (chuanhe, ruoyushi, shuigenjiang,tianyizhang))"
        operation_week_jira_jql['OB_all_jql'] = OB_all_jql
        OT_all_jql = "project = OT AND 部门 in (Infra, Front-end)  AND (component  in (运营系统)\
            OR assignee in (chuanhe, ruoyushi, shuigenjiang,tianyizhang) OR reporter in (chuanhe, ruoyushi, shuigenjiang,tianyizhang))\
                ORDER BY status DESC, created DESC    "
        operation_week_jira_jql['OT_all_jql'] = OT_all_jql
        OT_week_jql = "project = OT AND 部门 in (Infra, Front-end) AND (component in (运营系统)\
            OR assignee in (chuanhe, ruoyushi, shuigenjiang,tianyizhang) OR reporter in (chuanhe, ruoyushi, shuigenjiang,tianyizhang)) and created >= " + \
            last_week_begin+" AND created <= "+last_week_end + \
            " ORDER BY status DESC, created DESC"
        operation_week_jira_jql['OT_week_jql'] = OT_week_jql
        OB_week_jql = "project = OB AND created >= " + last_week_begin + " AND created <= " + last_week_end + \
            " AND (component in (Visualizer, 元启行, 元启行后台) OR assignee in (chuanhe, ruoyushi, shuigenjiang,tianyizhang)) \
                ORDER BY key DESC, priority DESC, updated DESC"
        operation_week_jira_jql['OB_week_jql'] = OB_week_jql
        OP_all_jql = "project = OP AND issuetype = 运营Bug AND\
             (assignee in (chuanhe, tianyizhang, ruoyushi, tianyizhang, shuigenjiang) or\
                reporter in (chuanhe, ruoyushi, tianyizhang, shuigenjiang)) ORDER BY priority DESC, updated DESC"
        operation_week_jira_jql['OP_all_jql'] = OP_all_jql
        Dev_all_jql = "project = OP AND issuetype = DevBug AND\
             (assignee in (chuanhe, tianyizhang, ruoyushi, shuigenjiang)\
                OR reporter in (chuanhe, ruoyushi, tianyizhang, shuigenjiang)) ORDER BY priority DESC, updated DESC"
        operation_week_jira_jql['Dev_all_jql'] = Dev_all_jql
        last_day, today = self.dealweek.get_day_time(1)
        OB_dayly_jql = "project = OB AND created >= "+last_day + \
            " AND created <= "+today+" order by created DESC"
        operation_week_jira_jql['OB_dayly_jql'] = OB_dayly_jql
        return operation_week_jira_jql

    def operation_dayly_jql(self):
        operation_dayly_jql = {}
        last_day, now_day = self.dealweek.get_day_time(1)
        OB_dayly_jql = "project = OB AND created >= " + last_day + " AND created <= " + now_day + \
            " ORDER BY key DESC, priority DESC, updated DESC"
        operation_dayly_jql['OB_dayly_jql'] = OB_dayly_jql
        return operation_dayly_jql


if __name__ == '__main__':
    sjj = SetJiraJql()
    operation_dayly_jql = sjj.operation_dayly_jql()
    operation_week_jira_jql = sjj.operation_week_jira_jql
    print(operation_dayly_jql)
    print(operation_week_jira_jql)
