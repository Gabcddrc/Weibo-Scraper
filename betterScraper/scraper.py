import codecs
import collections
import copy
import csv
import json
import logging
import logging.config
import math
import os
from pathlib import Path
import random
import sys
import warnings
from collections import OrderedDict
from datetime import date, datetime, timedelta
from time import sleep

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from requests.api import request
from tqdm import tqdm


userIDs = ['1669879400']
from_date = datetime.strptime('2021-9-1', '%Y-%m-%d')
to_date = datetime.strptime(str(date.today()), '%Y-%m-%d')
keyWords=''


collection = []

def standardize_date(created_at):
    """标准化微博发布时间"""
    if u'刚刚' in created_at:
        created_at = datetime.now().strftime('%Y-%m-%d')
    elif u'分钟' in created_at:
        minute = created_at[:created_at.find(u'分钟')]
        minute = timedelta(minutes=int(minute))
        created_at = (datetime.now() - minute).strftime('%Y-%m-%d')
    elif u'小时' in created_at:
        hour = created_at[:created_at.find(u'小时')]
        hour = timedelta(hours=int(hour))
        created_at = (datetime.now() - hour).strftime('%Y-%m-%d')
    elif u'昨天' in created_at:
        day = timedelta(days=1)
        created_at = (datetime.now() - day).strftime('%Y-%m-%d')
    else:
        created_at = created_at.replace('+0800 ', '')
        temp = datetime.strptime(created_at, '%c')
        created_at = datetime.strftime(temp, '%Y-%m-%d')
    return created_at


#retrieve weibo json data
def get_user_Json(param):
    url = 'https://m.weibo.cn/api/container/getIndex?'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
    headers = {'User_Agent': user_agent, 'Cookie': "not necessary"}

    r = requests.get(url,
                        params= param,
                        headers= headers,
                        verify=False)
    return r.json()

def retrieve_user_info(userID):
    userJson = get_user_Json({'containerid': '100505' + userID})
    userProfile = {}
    if userJson['ok']:
        userInfo = userJson['data']['userInfo']
        userProfile['userID'] = userID
        userProfile['name'] = userInfo.get('screen_name', '')
        userProfile['num_of_followers'] = userInfo.get('followers_count', '')
        userProfile['follow_count'] = userInfo.get('follow_count', '')
        userProfile['weibo_count'] = userInfo.get('statuses_count', '')
        return userProfile

    else:
        print('user might be banned')
        return None



def retrieve_page(page_num, userID):
    try: 
        if keyWords:
            params = {
                'container_ext': 'profile_uid:' + userID,
                'containerid': '100103type=401&q=' + keyWords,
                'page_type': 'searchall'
            } 
        else:
            params = { 'containerid': '107603' + userID }

        params['page'] = page_num
        page_json = get_user_Json(params)
        if page_json['ok']:
            weibos = page_json['data']['cards']

        for weibo in weibos:
            if weibo['card_type'] == 9:

                weiboInfo = weibo['mblog']

                #ignore long text
                if weiboInfo.get('pic_num') <= 9:
                    text = weiboInfo['text']
                    selector = etree.HTML(text)

                    created_at = datetime.strptime(standardize_date(weiboInfo['created_at']), '%Y-%m-%d')
                    if created_at < from_date:
                        return 1
                    if created_at > to_date:
                        return 2
                    collect = {'weibo_Id' : weiboInfo['id'], 'content' : selector.xpath('string(.)'), 'date' : standardize_date(weiboInfo['created_at']), 'type' : 'post' }
                    collection.append(collect)
                    get_comments(weiboInfo['id'], 1)
            else:
                print('skipping some weibo')

        return 0
    except Exception as e:
        print(e)
        return None


def get_comments(weibo_Id, num_of_comments):

    comments_url = "https://m.weibo.cn/api/comments/show?id={id}&page={page}".format(id=weibo_Id, page=1)
    comments_req = requests.get(comments_url)
    try: 
        comments_json = comments_req.json()
    except Exception as e:
        print('fail to grab comment')
        return None

    comments_data = comments_json.get('data')
    if not comments_data:
        print('no comments')
        return None
    comments = comments_data.get('data')
    
    for comment in comments:
        num_of_comments -= 1
        
        text = comment['text']
        selector = etree.HTML(text)
        collect = {'weibo_Id' : comment['id'], 'content' : selector.xpath('string(.)'), 'date' : standardize_date(comment['created_at']), 'type' : 'comment' }
        collection.append(collect)

        if num_of_comments <= 0:
            return None

    

def scrape():
    try:
        if to_date >= from_date:
            print('valid time period')
            for userID in userIDs:
                userProfile = retrieve_user_info(userID)

                weibo_count = userProfile['weibo_count']
                page_count = int(math.ceil(weibo_count / 10.0))

                randomSleep = 0
                random_pages = random.randint(1, 5)
                
                for page_num in tqdm(range(1, page_count+1)):
                    status = retrieve_page(page_num, userID) # 1 end of the time period,  2 haven't reached the set time period
                    if status == 1:
                        break
                    if status == 2:
                        page_num += 5
                        continue

                    
                    #mimic human 
                    if (page_num - randomSleep) % random_pages == 0:
                            sleep(random.randint(6, 10))
                            randomSleep = page_num
                            random_pages = random.randint(1, 5)
        else:
            print('invalid time period')

    except Exception as e:
        print(e)
        
scrape()

import pandas as pd
pd.DataFrame(collection).to_csv('weibos.csv')