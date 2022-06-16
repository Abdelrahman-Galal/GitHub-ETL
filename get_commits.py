#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" - Developed and Tested on Python 3.7.4 - PostgreSQL 12
    - Recommned to create virtual env > python3 -m venv /pass/to/env
                                      > activate.bat ( Windows )
    - You may reuiquire to install some libs  > pip install requests
                                              > pip install psycopg2
    - Tested on direct locally hosted DB without docker image 

    - Tested on URL https://api.github.com/repos/apache/airflow/commits
                 
"""
"""Get Top Commiters to a Github Project."""
import argparse
import datetime
import requests
import base64
from psycopg2 import connect, Error


def get_commits(date,url):
    #convert date string to datetime
    end_date = datetime.datetime(int(date[:4]),int(date[4:6]),int(date[6:]),23,59,59)
    #start date in the first day in month day = 1   
    start_date = end_date.replace(day=1,hour=0,minute=0,second=0)
    print(f"Fetching date from {start_date} to {end_date}")
    #By defualt page size 30 , modifing it to be 100 by coding per_page=100( max allowed values by the API )
    url = f'{url}?per_page=100'
    # Get the commits from the first day of month till the given date
    response = requests.get(url,params= {'since':start_date,'until':end_date})
    #Serilaize the response to json dict
    json_response = response.json()
    #Looping on the header link to get all responses ( more than the first 100 ) the last key rel = 'last'
    while 'next' in response.links.keys():
      response=requests.get(response.links['next']['url'])
      json_response.extend(response.json())
    #Create two lists to parse commits and author required values
    data_commit = list() 
    data_author = list()
    #Loop through the response and extract required value , FK of author is marked as -1
    for r in json_response:
        row_commit = (r['sha'],r['url'],r['commit']['message'],r['commit']['author']['date'],-1,r['author']['login'],datetime.date.today())
        email_company = r['commit']['author']['email']
        email_company = email_company[email_company.find('@')+1:email_company.find('.',email_company.find('@'))]
        row_author = (r['author']['login'],r['commit']['author']['email'],email_company,datetime.date.today())
        data_commit.append(row_commit)
        data_author.append(row_author)
    print(f"{len(data_commit)} commits fetched from GitHub")
    print("Calling Database to save output")  
    #Invoke database function to insert to postgreSQL    
    database_operations(data_commit,data_author)   
    
    #Another library to do the same is github
    #from github import Github
    #g = Github()
    #repo = g.get_repo("apache/airflow")
    #commits = repo.get_commits(since=end_date, until=start_date)
    #for commit in commits:
    #    print(commit.sha)
    #    print(commit.commit.author)
    #print(commits[0].commit.author)
    
def database_operations(data_commit,data_author):
    try:
    # declare a new PostgreSQL connection object
        conn = connect(
        dbname = "docker",
        user = "docker",
        host = "127.0.0.1",
        #encoded password
        password = base64.b64decode("ZG9ja2Vy").decode("utf-8"),
        # attempt to connect for 15 seconds then raise exception
        connect_timeout = 15
        )
        cur = conn.cursor()
        print ("created cursor object:", cur)
    except (Exception, Error) as err:
        print ("psycopg2 connect error:", err)
        conn = None
        cur = None
    if cur != None:
        try:
           #Insert to table AIRFLOW_COMMITS if no coflict with existing COMMIT_URL to avoid duplicated records
            sql_insert_commits = """ INSERT INTO COMMITS 
                                     VALUES(%s,%s,%s,%s,%s,%s,%s) 
                                     ON CONFLICT (COMMIT_URL) DO NOTHING"""
            #Insert to table AIRFLOW_AUTHOR if no coflict with existing LOGIN_NAME to avoid duplicated records
            sql_insert_authors = """INSERT INTO AUTHOR (LOGIN_NAME,EMAIL,EMAIL_COMPANY,CREATION_DATE) 
                                    VALUES(%s,%s,%s,%s) 
                                    ON CONFLICT (LOGIN_NAME) DO NOTHING"""
            #Update fk for newly added records marked as AUTHOR_ID = -1 
            sql_update_fk_commits= """UPDATE COMMITS c SET AUTHOR_ID = 
                                        (SELECT id 
                                        FROM AUTHOR r 
                                        WHERE c.LOGIN_NAME = r.LOGIN_NAME)
                                        WHERE  c.AUTHOR_ID = -1"""
            #Bulk Insert using  executemany                                 
            cur.executemany(sql_insert_commits,data_commit)
            #Number of newly (not duplicated) rows
            new_commits_inserted= cur.rowcount
            cur.executemany(sql_insert_authors,data_author)
            new_authors_inserted= cur.rowcount
            cur.execute(sql_update_fk_commits)
            conn.commit()
            print(f" Inserted {new_commits_inserted} new commits")
            print(f" Inserted {new_authors_inserted} new authors")
            print ('finished Database Operation Successfully!')
        except (Exception, Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()

    # close the cursor and connection
    cur.close()
    conn.close()
    print ('Closed Connection')


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Get the commits at a given date")
    parser.add_argument('-d',
                        '--date',
                        type=str,
                        help='The date to be executed in the YYYYMMDD format. (Eg. 20180120)',
                        required=True)
      
    parser.add_argument('-u',
                        '--url',
                        type=str,
                        help='url of the repo in the form https://api.github.com/repos/apache/airflow/commits',
                        required=True)
    args = parser.parse_args()
    try:
        int(args.date)
    except:
        print("The date has to be all numbers in YYYYMMDD format")
        exit()
    if len(args.date) != 8:
        print("The date has to be 8 numbers in YYYYMMDD format")
        exit()
    else:
       get_commits(args.date,args.url)