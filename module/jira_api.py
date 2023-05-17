import plotly.graph_objs as go
from jira import JIRA
import pandas as pd
import datetime
from IPython.display import display, HTML

USER = 'simulationTest0'
PASSWD = '123456Qq.'


class JiraAPI(object):
    '''
    query issues to dataframe
    '''
    def __init__(self):
        self.jira = self.authority(USER, PASSWD)

    def authority(self, user, passwd):
        options = {"server": "https://jira.deeproute.ai/"}
        jira = JIRA(options, basic_auth=(user, passwd))
        return jira

    def search_issues(self, jql):
        block_size = 1000
        block_num = 0
        all_issues = []
        while True:
            start_idx = block_num * block_size
            issues = self.jira.search_issues(jql, start_idx, block_size)
            if len(issues) == 0:
                break
            block_num += 1
            for issue in issues:
                all_issues.append(issue)
        return all_issues

    def deal_data(self, jql):
        all_issues = self.search_issues(jql)
        issue_dicts = []
        for issue in all_issues:
            issue_dict = {
                'key': issue.key,
                'summary': issue.fields.summary,
                'assignee': issue.fields.assignee,
                'status': issue.fields.status.name,
                #'reporter': issue.fields.reporter,
                'created': issue.fields.created,
                #'priority': issue.fields.priority.name,
                'components': [c.name for c in issue.fields.components],
                'labels': issue.fields.labels
            }
            issue_dicts.append(issue_dict)

        data_frame = pd.DataFrame(issue_dicts)
        data_frame['created'] = data_frame['created'].str.replace("T", " ")
        data_frame['created']=[x[:19] for x in data_frame['created']]
        data_frame['components'] = data_frame['components'].astype(str).str.replace("[", "", regex=True)
        data_frame['components'] = data_frame['components'].astype(str).str.replace("]", "", regex=True)
        data_frame['components'] = data_frame['components'].astype(str).str.replace("'", "", regex=True)
        data_frame['labels'] = data_frame['labels'].astype(str).str.replace("[", "", regex=True)
        data_frame['labels'] = data_frame['labels'].astype(str).str.replace("]", "", regex=True)
        data_frame['labels'] = data_frame['labels'].astype(str).str.replace("'", "", regex=True)
        return data_frame


if __name__ == '__main__':

    # jql = 'project = OB AND created >= ' + yesterday + \
    #     ' AND created <= ' + today + ' ORDER BY priority DESC, updated DESC'
    jql = 'project = OB AND created >= 2021-11-13 AND created < 2021-11-14 ORDER BY created DESC'
    ja = JiraAPI()
    print(ja.deal_data(jql))
