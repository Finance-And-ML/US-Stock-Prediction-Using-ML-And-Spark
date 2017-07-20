from bs4 import BeautifulSoup
import requests, requests.utils
import json
import time
from datetime import datetime, timedelta, date
import pickle
import os

def getArchiveURLs(archiveURL):
    r= requests.get(archiveURL, verify = False)
    soup = BeautifulSoup(r.content, 'lxml')
    for each in soup.select('.newsItem')[0].findAll('a'):
        yield each['href']

def getArticle(urls): #url list
    with requests.session() as rs:
        headers = {
            "authority":"www.wsj.com",
            "method":"GET",
            "path":"/articles/investors-beware-bond-prices-are-threatening-the-stock-market-1499714256?mod=nwsrl_streetwise",
            "scheme":"https",
            "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "accept-encoding":"gzip, deflate, br",
            "accept-language":"zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2",
            "cache-control":"max-age=0",
            "cookie":"djcs_route=18091b1f-2c73-4a93-ab10-77b0d4d4f9d3; __gads=ID=071e7398f896d5e9:T=1499313217:S=ALNI_MbbeOCEcDUvvhoYbikRrj16_l9-yw; usr_bkt=63L1D4y2F9; test_key=0.757460292271801; optimizelyEndUserId=oeu1499313249184r0.06107342412353045; vidoraUserId=q0pik87oi1l3tv3unahk66636fv8na; AMCV_CB68E4BA55144CAA0A4C98A5%40AdobeOrg=1999109931%7CMCMID%7C43903462061989793773070211523986560450%7CMCAAMLH-1500410214%7C7%7CMCAAMB-1500410214%7CNRX38WO0n5BH8Th-nqAG_A%7CMCAID%7C2CAED21805079FB6-40000111A0014D53%7CMCIDTS%7C17359%7CMCOPTOUT-1499805353.914%7CNONE%7CMCSYNCSOP%7C411-17361%7CvVersion%7C2.1.0; __utma=186241967.2131161835.1499313322.1499994634.1500010172.8; __utmz=186241967.1499313322.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); Ooyala=o|19:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|2:1500010175&dsYW4yYjE6eMEmXYeaxaINGe8ebEBgC2|1:1500010175&o|1:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|1:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|19:1500010175&o|2:1500010175&dsYW4yYjE6eMEmXYeaxaINGe8ebEBgC2|28:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|28:1500010175&o|28:1500010175&dsYW4yYjE6eMEmXYeaxaINGe8ebEBgC2|2:1500010175&hyNmJrYTE68pFGWJ_GxPJoBJtUHIWcGA|1:1499383520&hyNmJrYTE68pFGWJ_GxPJoBJtUHIWcGA|28:1499383520&hyNmJrYTE68pFGWJ_GxPJoBJtUHIWcGA|2:1499383520; cke=%7B%22A%22%3A11%2C%22G%22%3A%2217%22%7D; dji_user=c010b073-1ea4-40a4-bbcb-1ebf7a982ada; cX_S=j58cbotfpw05hspa; cX_G=cx%3A12tit3mo683o6qlihfmrc4ukw%3A3nec94y8xyzg6; DJSESSION=continent%3dna%7c%7czip%3d10001%2d10014%7c%7ccountry%3dus%7c%7cregion%3dny%7c%7cORCS%3dna%2cus%7c%7ccity%3dnewyork%7c%7clongitude%3d%2d73.9967%7c%7ctimezone%3dest%7c%7clatitude%3d40.7500; spotlightSet=true; _ncg_g_id_=5251b8e5-85ef-49ab-9cb6-895924a97ffa; DJCOOKIE=ORC%3Dna%2Cus; hok_seg=8m5mj1igy96k,8mgp602uzogr,8mgpwi3dgut2,8n2vtnp32stn,8o024avhbmrb,8ob3yiqn1vvh,8ob3yiqn1vvm,8ob56qt73nfj,8ob6ryzbkb8a,8olzc9gj072g,8om721mpwp1g,8om721mpwp1j,8om7oh4vq6f8; s_vnum=1501905217437%26vn%3D35; s_vmonthnum=1501560000005%26vn%3D31; s_fid=285CD886C3797AF3-17AC0A56D3BE1AE4; wsjregion=na%2Cus; optimizelySegments=%7B%227731792487%22%3A%22true%22%2C%225107762377%22%3A%22gc%22%2C%225119130028%22%3A%22search%22%2C%225119130029%22%3A%22none%22%2C%228419251058%22%3A%22true%22%2C%225102690584%22%3A%22false%22%2C%228353100649%22%3A%22true%22%2C%228412721504%22%3A%22true%22%2C%228412531110%22%3A%22true%22%2C%228417010251%22%3A%22true%22%7D; optimizelyBuckets=%7B%225911560489%22%3A%225915450169%22%2C%228427202117%22%3A%228427400077%22%7D; ResponsiveConditional_initialBreakpoint=lg; utag_main=v_id:015d160820ab000a88a4e328e8dc05079003507100bd0$_sn:35$_ss:0$_st:1500313235192$vapi_domain:wsj.com$_pn:7%3Bexp-session$ses_id:1500310679090%3Bexp-session; s_monthinvisit=true; s_invisit=true; gpv_pn=WSJ_Article_Politics_20170716_Republican%20Health%20Bill%20to%20Be%20Delayed%20as%20Sen.%20John%20McCain%20Recovers%20From%20Surgery%20%20%20; s_cc=true; _ncg_id_=15d1663300f-a5411480-cc1a-4195-9514-54b54f6fc0d7; _ncg_sp_id.5378=5131c27e-1075-4df8-9b79-a5a7d53c4464.1499313219.34.1500311435.1500308712.599efbdd-a1cb-4032-bce4-3fc22182b2e9; _ncg_sp_ses.5378=*; ki_t=1499313262608%3B1500307482494%3B1500311435502%3B8%3B156; ki_r=; NaN_hash=a829ca4fRPCDRVLK1499309105519; _mibhv=anon-1499313262868-9345571041_4171; cX_P=j4rwdnnkmbi9kliu; bkuuid=xmVpmR8B99eLpVPj; sc.ASP.NET_SESSIONID=vjhamv10ueqghgjzmrv4r0rp; optimizelyPendingLogEvents=%5B%22n%3Doptly_activate%26u%3Doeu1499313249184r0.06107342412353045%26wxhr%3Dtrue%26time%3D1500311433.27%26f%3D5905990923%2C8454662030%2C8334291247%2C5911560489%2C8326732335%2C8427202117%2C8378002380%2C8461011293%2C8330810237%2C8373822077%26g%3D%22%5D; s_sq=djglobal%2Cdjwsj%3D%2526pid%253DWSJ_Article_Politics_20170716_Republican%252520Health%252520Bill%252520to%252520Be%252520Delayed%252520as%252520Sen.%252520John%252520McCain%252520Recovers%252520From%252520Surgery%252520%252520%252520%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Faccounts.wsj.com%25252Flogin%25253Ftarget%25253Dhttp%2525253A%2525252F%2525252Fwww.wsj.com%2525252Farticles%2525252Frepublican-health-bill-t%2526ot%253DA; _parsely_slot_click={%22url%22:%22https://www.wsj.com/articles/republican-health-bill-to-be-delayed-as-sen-john-mccain-recovers-from-surgery-1500173998%22%2C%22x%22:1279%2C%22y%22:70%2C%22xpath%22:%22//*[@id=%5C%22full-header%5C%22]/div[1]/div[1]/div[1]/header[1]/div[1]/div[1]/div[1]/div[1]/a[2]%22%2C%22href%22:%22https://accounts.wsj.com/login?target=http%253A%252F%252Fwww.wsj.com%252Farticles%252Frepublican-health-bill-to-be-delayed-as-sen-john-mccain-recovers-from-surgery-1500173998%22}; _parsely_session={%22sid%22:32%2C%22surl%22:%22https://www.wsj.com/%22%2C%22sref%22:%22https://www.google.com/%22%2C%22sts%22:1500307459811%2C%22slts%22:1500067652271}; _parsely_visitor={%22id%22:%2269cd7ca0-9b6c-495b-98b3-eb4cf869213d%22%2C%22session_count%22:32%2C%22last_session_ts%22:1500307459811}; GED_PLAYLIST_ACTIVITY=W3sidSI6IjlVSVIiLCJ0c2wiOjE1MDAzMTE0NDksIm52IjowLCJ1cHQiOjE1MDAzMTA5NDAsImx0IjoxNTAwMzExMTU5fV0.; run-sso=true; user_type=subscribed; TR=V2-f8b7ec452daa7484a5b670937b4cee09d7b04d672d0380fd5c0254ff895b4446; REMOTE_USER=12992a41-d64d-4490-9c0e-9c9683b5b82b; djcs_auto=M1500239024%2F6nbea5RL9Jhg3i4tVOYqPUQQlN0S1muVmomcwDQSQviB%2BwNmopkyvU065P%2FQsBGA5S%2FLNyxj167fHJOzTVDMRBc8i1LWPfo9XPZOkDLibK3hGlCZL2HqJ17ZxoARydcY9G8vvOMAtAcMgBB1BztMTdYgT0NfnMdA6D9iTBUPHnH1KaDCqKebXP8be4PRgH18Jyt2YSmzWcOLcxTqRCYm0Qj5hCEVjWK%2B4fWZ4kt3ENCNIyeRgytTt7g8cLyKq5vt7UrMsUoi9AWWSVHnd7wf4w%3D%3DG; djcs_sid=M1500309828%2Fb5irQFBUrFbDfqPGwp0jpuOKV75pX%2B42WMMDntwxOhStRaTgB%2F2ZuZ6T%2B89mnLlqX2XLQgFKmA7%2BH6czbkoR8AYGfIYEDOk9lcoicTZVpJI%3DG; djcs_session=M1500309828%2FeXNp8HLhsClpFndj%2FPY1icAQFr79v37Zk4%2FMeWFanVP2OrMu9VbqEGL02ofax11Gv3V34JRdLuMQKoK5dqzTOjqgdt02W%2FFFoiMVEvgshnd%2F%2BPj2CIlCfzrnNNL7p4lu%2BzasDwGrsDFSX8UDcKszAqwEiBm5AZTzAQN8IWyQX21l1YDCcpD94XPQIPPcK1QL69dNAaS%2FwDsvQICiS9XJOwJCjW59EZs4z%2FZkXEhj5jpjDZABhQkMsT00Ftw1L0pRjbm57CL3%2FJRPzw9grZU%2FExYmyVsneUcrxv6wRpqhpCHtwI1V241aExI5W%2BqzfkCQPPPS9CBZmY%2FmR0%2BOjUfAiAB8HIDV0t9RZPB%2BBgUdxeTP82osl8G8Bl9cFdt4Pexy%2B8Yft73ShEDQGcTV6ArH27kAs5MDKm8Drtbno2w0NWB2ykylSZGy4aSB67RxISmMBwLOcWWWmxqsrRNzwbIqHUMaiZc1RT5G1Hw%2FqqNV6dA%3DG; djcs_info=eydsYXN0X25hbWUnOidZYW5nJywnc2Vzc2lvbic6J2YyMDYwZTM2LTk5NzktNDk5MC1iYzYyLTQyMjI0M2JjNWQ1NCcsJ3JvbGVzJzonV1NKJTJDV1NKLUFSQ0hJVkUlMkNGUkVFUkVHLUJBU0UlMkNXU0otTU9CSUxFJTJDTlAtU0VSVklDRSUyQ0ZSRUVSRUctSU5ESVZJRFVBTCUyQ1dTSi1TRUxGU0VSVicsJ3V1aWQnOicxMjk5MmE0MS1kNjRkLTQ0OTAtOWMwZS05Yzk2ODNiNWI4MmInLCdlbWFpbCc6J2N5eTI5MiU0MG55dS5lZHUnLCdmaXJzdF9uYW1lJzonQ2h1bi1ZaScsJ3VzZXItdGFncyc6J1dTSi1TVUInLCd1c2VyJzonY3l5MjkyJTQwbnl1LmVkdScsJyN2ZXInOicxJ30%3D",
            "upgrade-insecure-requests":"1",
            "user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
        }
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
                    keywords = keywordsTag['content']
                    time.sleep(1)
                    yield {'news_time':artTime, 'news_title':title, 'content':article, \
                            'source':'wsj', 'keywords' : keywords}
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
    preURL = 'http://www.wsj.com/public/page/archive-'
    start_date = date(2017, 6, 7)
    end_date = date(2017, 6, 8)
    for single_date in daterange(start_date, end_date):
        with open('log\wsjScrap.log', 'a') as log:
            log.write('---{0}\n'.format(single_date.strftime('%Y-%m-%d')))
        hrefList = []
        hrefList.extend(getArchiveURLs(preURL + single_date.strftime('%Y-%-m-%d') + '.html'))

        wsjArticleList = []
        wsjArticleList.extend(getArticle(hrefList))

        with open('wsjArticle/{0}.json'.format(single_date.strftime('%Y-%m-%d')), 'w') as p:
            json.dump(wsjArticleList, p)
