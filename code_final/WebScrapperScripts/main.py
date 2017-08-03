import wsjScrap
import Reuter_News_Scrapper
import pickle
from datetime import datetime, timedelta, date
import json
import requests
import os

def get_Reuters_urls(back_time):
    reuters_API = requests.get('https://newsapi.org/v1/articles?source=reuters&sortBy=latest&apiKey=a4e3033bc0254b248603036c4f2150fb')
    reuters_API_json = json.loads(reuters_API.content.decode('utf-8'))

    for each in reuters_API_json['articles']:
        publishTime = datetime.strptime(each['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
        if back_time > publishTime:
            return
        yield each['url']

def get_wsj_urls(back_time):
    wsj_API = requests.get('https://newsapi.org/v1/articles?source=the-wall-street-journal&sortBy=top&apiKey=a4e3033bc0254b248603036c4f2150fb')
    wsj_API_json = json.loads(wsj_API.content.decode('utf-8'))

    for each in wsj_API_json['articles']:
        publishTime = datetime.strptime(each['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
        if back_time > publishTime:
            continue
        yield each['url']

if __name__ == '__main__':
    os.chdir('/home/cyy292/US-Stock-Prediction-Using-ML-And-Spark/')
    current_time = datetime.now()
    back_time = current_time - timedelta(minutes = 15)

    articles = []

    articles.extend(wsjScrap.getArticle(get_wsj_urls(back_time)))
    articles.extend(Reuter_News_Scrapper.getNewsContent(get_Reuters_urls(back_time)))

    with open('jsonFile/{0}.json'.format(current_time.strftime('%Y-%m-%d-%H-%M')), 'w') as j:
        json.dump(articles, j)
