from google.cloud import bigquery
import os
import warnings
import pandas as pd
import config
from termcolor import colored
import json
import requests
from pandas.io.json import json_normalize
import numpy as np
import time
import argparse


## Initilize a command-line option parser.
parser = argparse.ArgumentParser()

parser.add_argument('--repo_id', type=float, help='Repository ID')
## Parse the command-line option.
args = parser.parse_args()

warnings.filterwarnings("ignore")

github_api = "https://api.github.com"
gh_session = requests.Session()
gh_session.auth = (config.GITHUB_USERNAME, config.GITHUB_TOKEN)

# pull data for repo from bigquery
project_id = 'uic-capstone-int'
client = bigquery.Client(project=project_id)

id = args.repo_id

def commits_of_repo_github(repo, owner, api):
    commits = []
    next = True
    i = 1
    while next == True:
        url = api + '/repos/{}/{}/commits?page={}&per_page=100'.format(owner, repo, i)
        commit_pg = gh_session.get(url = url)
        commit_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in commit_pg.json()]    
        commit_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in commit_pg_list]
        commits = commits + commit_pg_list
        if 'Link' in commit_pg.headers:
            if 'rel="next"' not in commit_pg.headers['Link']:
                next = False
        i = i + 1
    return commits

def create_commits_df(repo, owner, api):
    commits_list = commits_of_repo_github(repo, owner, api)
    return json_normalize(commits_list)

def RESTdb(repo_name_list):

      repo_name_df = pd.DataFrame(repo_name_list,columns=['owner_repo'])
      repo_name_df[['owner','repo']] = repo_name_df.owner_repo.str.split("/",n=1,expand=True)
      get_commit = pd.DataFrame()
      for k in range(0,len(repo_name_df)):
        
        
        try:
            
            REST_df = create_commits_df(repo_name_df.iloc[k].repo, repo_name_df.iloc[k].owner, github_api)
            REST_df = REST_df[['sha','commit.committer.date','commit.committer.email']]
            REST_df['commit.committer.date'] = pd.to_datetime(REST_df['commit.committer.date'])
            REST_df['commit.committer.date'] = REST_df['commit.committer.date'].dt.date
            REST_df.rename({'commit.committer.date':'committed_date','commit.committer.email':'email', 'sha':'commit'}, axis=1, inplace=True)
            get_commit = get_commit.append(REST_df, ignore_index=True)
            
        except:
            continue
      return get_commit

df = pd.DataFrame()

sql='''

SELECT FORMAT_DATETIME("%Y-%m-%d", DATETIME(date)) AS year_month, repoID, repo_name, type, count (*) ct
FROM
(

SELECT type,created_at as date, repo.id as repoID, repo.name as repo_name
FROM githubarchive.year.2015
WHERE type IN ('PushEvent','IssuesEvent','ForkEvent','PullRequestEvent','WatchEvent','IssueCommentEvent','CommitComentEvent') AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT type,created_at as date, repo.id as repoID, repo.name as repo_name
FROM githubarchive.year.2016
WHERE type IN ('PushEvent','IssuesEvent','ForkEvent','PullRequestEvent','WatchEvent','IssueCommentEvent','CommitComentEvent') AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT type,created_at as date, repo.id as repoID, repo.name as repo_name
FROM githubarchive.year.2017
WHERE type IN ('PushEvent','IssuesEvent','ForkEvent','PullRequestEvent','WatchEvent','IssueCommentEvent','CommitComentEvent') AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT type,created_at as date, repo.id as repoID, repo.name as repo_name
FROM githubarchive.year.2018
WHERE type IN ('PushEvent','IssuesEvent','ForkEvent','PullRequestEvent','WatchEvent','IssueCommentEvent','CommitComentEvent') AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT type,created_at as date, repo.id as repoID, repo.name as repo_name
FROM githubarchive.year.2019
WHERE type IN ('PushEvent','IssuesEvent','ForkEvent','PullRequestEvent','WatchEvent','IssueCommentEvent','CommitComentEvent') AND repo.id IN UNNEST(@ids)
)
GROUP BY 1,2,3,4
ORDER BY 1;'''


job_configuration = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", id),]
)


dfx = client.query(
    sql,
    job_config=job_configuration
    ).to_dataframe()


#-------------------------Commit_author_count event creation-------------------

job_configuration = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", id),]
)

sql = '''
SELECT FORMAT_DATETIME("%Y-%m-%d", DATETIME(date)) AS year_month, repoID, repo_name, COUNT( DISTINCT a_id) AS ct
FROM (
    
SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2015
WHERE type = 'CommitCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2016
WHERE type = 'CommitCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2017
WHERE type = 'CommitCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2018
WHERE type = 'CommitCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2019
WHERE type = 'CommitCommentEvent' AND repo.id IN UNNEST(@ids)

)
GROUP BY 1,2,3
ORDER BY 1;'''

Cac = client.query(
    sql,
    job_config=job_configuration
    ).to_dataframe()
Cac['type'] = 'Commit_author_count'


#Union
dfx = pd.concat([dfx, Cac])


#-------------------------Issue_author_count event creation-------------------

job_configuration = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", id),]
)

sql = '''
SELECT FORMAT_DATETIME("%Y-%m-%d", DATETIME(date)) AS year_month, repoID, repo_name, COUNT( DISTINCT a_id) AS ct
FROM (
    
SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2015
WHERE type = 'IssueCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2016
WHERE type = 'IssueCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2017
WHERE type = 'IssueCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2018
WHERE type = 'IssueCommentEvent' AND repo.id IN UNNEST(@ids)

UNION ALL

SELECT created_at as date, repo.id as repoID, repo.name as repo_name, actor.id AS a_id
FROM githubarchive.year.2019
WHERE type = 'IssueCommentEvent' AND repo.id IN UNNEST(@ids)

)
GROUP BY 1,2,3
ORDER BY 1;'''

Iac = client.query(
    sql,
    job_config=job_configuration
    ).to_dataframe()
Iac['type'] = 'Issue_author_count'


#Union
dfx = pd.concat([dfx, Iac])

counter =1

for i in range(0,len(id)):
  print(colored(counter,color='white'))
  counter+=1

  dfPushes = dfx.loc[(dfx['type'] == 'PushEvent') & (dfx['repoID']==id[i])]
  dfPushes.rename(columns={'ct':'number_of_pushes'}, inplace=True)
  repo_name_list = dfPushes['repo_name'].unique().tolist()

  dfForks = dfx.loc[(dfx['type'] == 'ForkEvent') & (dfx['repoID']==id[i])]
  dfForks.rename(columns={'ct':'number_of_forks'}, inplace=True)
  
  dfPulls = dfx.loc[(dfx['type'] == 'PullRequestEvent') & (dfx['repoID']==id[i])]
  dfPulls.rename(columns={'ct':'number_of_pulls'}, inplace=True)
  
  dfBookmarks = dfx.loc[(dfx['type'] == 'WatchEvent') & (dfx['repoID']==id[i])]
  dfBookmarks.rename(columns={'ct':'number_of_bookmarks'}, inplace=True)


  dfIssues = dfx.loc[(dfx['type'] == 'IssuesEvent') & (dfx['repoID']==id[i])]
  dfIssues.rename(columns={'ct':'number_of_issues'}, inplace=True)


  dfIssuecomment = dfx.loc[(dfx['type'] == 'IssueCommentEvent') & (dfx['repoID']==id[i])]
  dfIssuecomment.rename(columns={'ct':'issue_comment_count'}, inplace=True)


  dfIssue_author_count = dfx.loc[(dfx['type'] == 'Issue_author_count') & (dfx['repoID']==id[i])]
  dfIssue_author_count.rename(columns={'ct':'issue_author_count'}, inplace=True)


  dfCommitcomment = dfx.loc[(dfx['type'] == 'CommitComentEvent') & (dfx['repoID']==id[i])]
  dfCommitcomment.rename(columns={'ct':'commit_comment_count'}, inplace=True)

  dfCommit_author_count = dfx.loc[(dfx['type'] == 'Commit_author_count') & (dfx['repoID']==id[i])]
  dfCommit_author_count.rename(columns={'ct':'commit_author_count'}, inplace=True)
  
  
  merge1 = pd.merge(dfPushes,dfForks,how='outer', on='year_month')
  merge2 = pd.merge(merge1, dfPulls, how='outer', on='year_month')
  merge3 = pd.merge(merge2, dfBookmarks, how='outer', on='year_month')
  merge4 = pd.merge(merge3, dfIssuecomment, how='outer', on='year_month')
  merge5 = pd.merge(merge4,dfCommitcomment,how='outer',on='year_month')
  merge6 = pd.merge(merge5, dfIssues, how='outer',on='year_month')
  merge7 = pd.merge(merge6, dfIssue_author_count, how='outer',on='year_month')
  merge8 = pd.merge(merge7, dfCommit_author_count, how='outer',on='year_month')
  merge8.drop(['repoID_x','repoID_y','type_x','type_y','type', 'repo_name_x','repo_name_y', 'repo_name'], axis=1, inplace=True)
  merge8['repoID']=id[i]
  merge8.sort_values(by='year_month',ascending=True,inplace=True)
  
  merge8['year_month']=pd.to_datetime(merge6['year_month'])
  merge8.rename({'year_month': 'datetime'}, axis=1, inplace=True)
  merge8.index=merge8['datetime']
  merge8.drop('datetime',axis=1,inplace=True)

  WeeklyTS = pd.DataFrame()
  WeeklyTS = merge8.resample('W').sum()
  WeeklyTS.reset_index(level=0, inplace=True)

  # repo_name_tuple = tuple(repo_name_list)


  dfCommits = RESTdb(repo_name_list)
  
  if dfCommits.empty:
    print(colored('No commits data for repoID: %d'%id[i], color='red'))
    continue
  else:
    print(colored('Successful Commits data Extraction from REST API for repoID: %d' %id[i], color='green'))


  dfCommits['committed_date']=pd.to_datetime(dfCommits['committed_date'])
  dfCommits.index=dfCommits['committed_date']
  dfCommits.drop('committed_date',axis=1,inplace=True)
  Weekly_commit_TS = pd.DataFrame()
  Weekly_commit_TS = dfCommits.resample('W').agg({'commit': 'count', 'email': 'nunique'})
  Weekly_commit_TS.rename(columns={"commit":"commit_count","email":"unique_committer_count"}, inplace=True)
  Weekly_commit_TS.reset_index(level=0, inplace=True)
  Weekly_commit_TS.rename(columns={'committed_date':'datetime'}, inplace=True)
  WeeklyTS = pd.merge(WeeklyTS,Weekly_commit_TS,how='outer', on='datetime')
  WeeklyTS.drop('repoID', axis=1, inplace=True)
  WeeklyTS.fillna(0, inplace=True)
  WeeklyTS['Total_count'] = WeeklyTS.drop('datetime',axis=1).sum(axis=1)
  

  # Setting Threshold in terms of percentage of drop
  Threshold = 0.1
  for j in range(5,len(WeeklyTS)):
    Avg = WeeklyTS['Total_count'].iloc[j-5:j,].mean()
    diff = WeeklyTS['Total_count'].iloc[j] - WeeklyTS['Total_count'].iloc[j-1]
    if diff>=0:
      WeeklyTS.loc[j,'Status'] = 1
    else:
      if abs(diff)/Avg < Threshold:
        WeeklyTS.loc[j,'Status'] = 1
      else:
        WeeklyTS.loc[j,'Status'] = 0


  WeeklyTS.index = WeeklyTS['datetime']
  WeeklyTS.drop('datetime', axis=1, inplace=True)
  WeeklyTS.insert(0, 'year', WeeklyTS.index.year)
  WeeklyTS.insert(1, 'month', WeeklyTS.index.month)
  WeeklyTS.insert(2, 'day', WeeklyTS.index.day)
  WeeklyTS.reset_index(level=0, inplace=True)
  WeeklyTS['repoID']=id[i]

  df=df.append(WeeklyTS, ignore_index=True)
# now creating the linear combination of features to calculate the health score
attributes = ['commit_count','number_of_bookmarks','number_of_pushes','number_of_forks','number_of_pulls','unique_committer_count','issue_comment_count','issue_author_count','number_of_issues','commit_comment_count','commit_author_count']
weights = [0.265982,0.151779,0.111914,0.098709,0.094123,0.090812,0.071414,0.059612,0.040631,0.007819,0.007206] # weights from fandom forest model

df.drop(['Total_count', 'Status'], axis=1, inplace=True)

df['Risk_Score'] = df[attributes].mul(weights).sum(1)
WeeklyTS = df[['year','month','day','repoID','Risk_Score']]
WeeklyTS = WeeklyTS[WeeklyTS.year>2014]
WeeklyTS.to_csv('data_pred')