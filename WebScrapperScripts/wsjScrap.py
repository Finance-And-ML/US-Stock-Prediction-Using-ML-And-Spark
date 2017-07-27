from bs4 import BeautifulSoup
from selenium import webdriver
import requests, requests.utils
import json
import time
from datetime import datetime, timedelta, date
import pickle
import os

def getCookie():
    driver = webdriver.Firefox()
    driver.get("https://www.wsj.com/")
    driver.find_element_by_link_text("Sign In").click()
    time.sleep(3)
    # driver.find_element_by_id("username").clear()
    driver.find_element_by_id("username").send_keys("cyy292@nyu.edu")
    # driver.find_element_by_id("password").clear()
    driver.find_element_by_id("password").send_keys("chunyi020")
    driver.find_element_by_css_selector("button.solid-button.basic-login-submit").click()
    cookie_list = driver.get_cookies()
    driver.close()

    cookie_Str = ""
    for cookie in cookie_list:
        if 'name' in cookie and 'value' in cookie:
            cookie_Str += cookie['name'] + "=" + cookie['value']+ "; "
    with open(parent_path +'meta/wsjCookie.txt') as j:
        j.write(cookie_Str[:-2])
    return cookie_Str[:-2]

def getArchiveURLs(archiveURL):
    r= requests.get(archiveURL, verify = False)
    soup = BeautifulSoup(r.content, 'lxml')
    for each in soup.select('.newsItem')[0].findAll('a'):
        yield each['href']

def getArticle(urls): #url list
    with open(parent_path + 'meta/wsjCookie.txt') as j:
        cookie = j.read()
    headers = {
        "authority":"www.wsj.com",
        "method":"GET",
        "path":"/articles/investors-beware-bond-prices-are-threatening-the-stock-market-1499714256?mod=nwsrl_streetwise",
        "scheme":"https",
        "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding":"gzip, deflate, br",
        "accept-language":"zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2",
        "cache-control":"max-age=0",
        "cookie":cookie ,
        "user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
    }
    c = requests.get('https://www.wsj.com/articles/sean-spicer-resigns-as-white-house-press-secretary-1500653457', headers = headers)
    soup = BeautifulSoup(c.content, 'lxml')
    if len(soup.select('.wsj-snippet-login')) != 0:
        headers['cookie'] = getCookie()

    with requests.session() as rs:
        for url in urls:
            try_count = 0
            while True:
                try:
                    r = rs.get(url, headers = headers)
                    soup = BeautifulSoup(r.content, 'lxml')
                    if len(soup.select('.article-wrap')) == 0:
                        break
                    if len(soup.select('.bigTop__hed')) > 0:
                        title = soup.select(".bigTop__hed")[0].text.strip()
                    else:
                        title = soup.select(".wsj-article-headline")[0].text
                    if 'What\'s News:' in title:
                        break
                    timeStr = soup.select(".timestamp")[0].text.strip()
                    if 'ET' not in timeStr:
                        break
                    if 'Updated' in timeStr:
                        timeStr = " ".join(timeStr.split(" ")[1:7]).strip()
                    artTime = str(datetime.strptime(''.join(timeStr.split('.'))[:-3], '%B %d, %Y %I:%M %p'))
                    article = ""
                    for each in soup.select(".article-wrap")[0].select('p'):
                        article += "" + each.text.strip()
                    article = "".join(article.split("\n"))
                    keywordsTag = soup.find("meta", attrs={"name":"keywords"})
                    if not keywordsTag:
                        break
                    keywords = [keywordsTag['content']]
                    time.sleep(1)
                    yield {'news_time':artTime, 'news_title':title, 'content':article, \
                            'url': url, 'keywords' : keywords}
                    break

                except Exception as e:
                    if try_count < 3:
                        time.sleep(5)
                        try_count +=1
                    else:
                        with open('log\wsjScrap.log', 'a') as log:
                            log.write('{0}, {1}'.format(url, str(e)))
                        break

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

if __name__ == '__main__':
    parent_path = os.getcwd()[:-18]
    preURL = 'http://www.wsj.com/public/page/archive-'
    start_date = date(2017, 7, 20)
    end_date = date(2017, 7, 27)
    for single_date in daterange(start_date, end_date):
        with open('log\wsjScrap.log', 'a') as log:
            log.write('---{0}\n'.format(single_date.strftime('%Y-%m-%d')))
        hrefList = []
        hrefList.extend(getArchiveURLs(preURL + single_date.strftime('%Y-%-m-%d') + '.html'))

        wsjArticleList = []
        wsjArticleList.extend(getArticle(hrefList))

        with open('{0}.json'.format(single_date.strftime('%Y-%m-%d')), 'w') as p:
            json.dump(wsjArticleList, p)
