from bs4 import BeautifulSoup
import urllib
import requests
import csv
import re
from datetime import datetime, timedelta
import time

def getNewsContent(urls):
    for url in urls:
        revision_date = ""
        title = ""
        keywords = ""
        sector = ""
        article = ""

        try_get_news_content_count = 0
        while True:
            try:
                r = requests.get(url)
                soup = BeautifulSoup(r.content, 'lxml')

                # These function use re.compile is too slow.
                # dateTag = soup.find_all("div", attrs={"class": re.compile("ArticleHeader_date.")})
                # headerTag = soup.find_all("h1", attrs={"class": re.compile("ArticleHeader_headline.")})

                dateTag = soup.find("meta", attrs={"name":"REVISION_DATE"})
                revision_date = dateTag['content']

                titleTag = soup.find("meta", attrs={"name":"sailthru.title"})
                title = titleTag['content']

                keywordsTag = soup.find("meta", attrs={"name":"keywords"})
                keywords = keywordsTag['content']

                sectorTag = soup.find("meta", attrs={"property":"og:article:section"})
                sector = sectorTag['content']

                articleTag = soup.find_all("div", attrs={"class": re.compile("ArticleBody_body.")})
                article = articleTag[0].text.replace('\n',' ').replace('\t',' ').replace('\r',' ').replace('\"',' ')
                yield {'news_time':convertTimestamp(revision_date), 'news_title':title, 'content':article, \
                        'source':'reuters', 'keywords' : keywords}
                break
            except:
                # print("Error in getNewsContent try_get_news_content_count = "+str(try_get_news_content_count))
                if try_get_news_content_count <= 3:
                    time.sleep(10)
                    try_get_news_content_count += 1
                else:
                    with open('log\reutersScrap.log', 'a') as log:
                        log.write('{0}, {1}'.format(url, str(e)))
                    break

        #return revision_date, title, keywords, sector, article

def getUrlsFromArchiveByDate(date):
    archivePageByDate = "http://www.reuters.com/resources/archive/us/"+date+".html"
    page = requests.get(archivePageByDate)
    soup = BeautifulSoup(page.content, 'lxml')

    moduleBody = soup.find_all("div", attrs={"class":"headlineMed"})

    url_list = []
    for i in range(len(moduleBody)):
        url = moduleBody[i].a["href"]
        if ("/news/picture/" in url) or ("/news/video/" in url) or ("article/pictures-report" in url) or ("/article/life-" in url):
            continue
        else:
            url_list.append(url)
    return url_list

def convertTimestamp(time_str):
    if len(time_str) == 0:
       return ""

    time_arr = time_str.split(" ")
    year_str = time_arr[5]
    time_zone = time_arr[4]
    minute = time_arr[3][:-3]
    day_str = time_arr[2]
    month_str = time_arr[1]

    time_to_convert = month_str+' '+day_str+' '+year_str+' '+minute+' '+time_zone

    utc_datetime_object = datetime.strptime(time_to_convert,'%b %d %Y %H:%M %Z')
    adjusted_EST_time = utc_datetime_object - timedelta(hours=4)

    return str(adjusted_EST_time)

def getCsvFileByDate(date):
    urls = getUrlsFromArchiveByDate(date)
    count = 0
    with open(date+".json", "w") as csv_file:
        csv_file.write('[\n')
        for url in urls:

            time_web, title, keywords, sector, article = getNewsContent(url)

            adjusted_time = convertTimestamp(time_web)
            adjusted_time = "\"news_time\":\"" + adjusted_time + "\""
            title = "\"news_title\":\"" + title + "\""
            keywords = "\"keywords\":\"" + keywords + "\""
            sector = "\"sector\":\"" + sector + "\""
            article = "\"content\":\"" + article + "\""
            url_encoded = "\"url\":\"" + url.encode('utf-8') + "\""
            line = '{' + adjusted_time + ',' + title + ',' + keywords + ',' + sector + ',' + article + ',' + url_encoded + '}'

            csv_file.write(line)
            csv_file.write('\n')
            count += 1
            print("Finished "+str(count)+" - "+url)

        csv_file.write(']')

    print('Completed! Total number of scrapped url from '+date+' is '+str(count))

if __name__ == '__main__':
    dates = ["20170705","20170704","20170703","20170702","20170701"]
    for date in dates:
         getCsvFileByDate(date)
