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
            "cookie":"djcs_route=18091b1f-2c73-4a93-ab10-77b0d4d4f9d3; __gads=ID=071e7398f896d5e9:T=1499313217:S=ALNI_MbbeOCEcDUvvhoYbikRrj16_l9-yw; usr_bkt=63L1D4y2F9; test_key=0.757460292271801; optimizelyEndUserId=oeu1499313249184r0.06107342412353045; vidoraUserId=q0pik87oi1l3tv3unahk66636fv8na; __utma=186241967.2131161835.1499313322.1499994634.1500010172.8; __utmz=186241967.1499313322.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); Ooyala=o|19:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|2:1500010175&dsYW4yYjE6eMEmXYeaxaINGe8ebEBgC2|1:1500010175&o|1:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|1:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|19:1500010175&o|2:1500010175&dsYW4yYjE6eMEmXYeaxaINGe8ebEBgC2|28:1500010175&BkbmQxOgC0f6WqmF9KW-eOntDoyc|28:1500010175&o|28:1500010175&dsYW4yYjE6eMEmXYeaxaINGe8ebEBgC2|2:1500010175&hyNmJrYTE68pFGWJ_GxPJoBJtUHIWcGA|1:1499383520&hyNmJrYTE68pFGWJ_GxPJoBJtUHIWcGA|28:1499383520&hyNmJrYTE68pFGWJ_GxPJoBJtUHIWcGA|2:1499383520; cke=%7B%22A%22%3A11%2C%22G%22%3A%2217%22%7D; dji_user=c010b073-1ea4-40a4-bbcb-1ebf7a982ada; cX_S=j58cbotfpw05hspa; DJSESSION=continent%3dna%7c%7czip%3d10001%2d10014%7c%7ccountry%3dus%7c%7cregion%3dny%7c%7cORCS%3dna%2cus%7c%7ccity%3dnewyork%7c%7clongitude%3d%2d73.9967%7c%7ctimezone%3dest%7c%7clatitude%3d40.7500; spotlightSet=true; hok_seg=8m5mj1igy96k,8mgp602uzogr,8mgpwi3dgut2,8n2vtnp32stn,8o024avhbmrb,8ob3yiqn1vvh,8ob3yiqn1vvm,8ob56qt73nfj,8ob6ryzbkb8a,8olzc9gj072g,8om721mpwp1g,8om721mpwp1j,8om7oh4vq6f8; AMCV_CB68E4BA55144CAA0A4C98A5%40AdobeOrg=1999109931%7CMCMID%7C43903462061989793773070211523986560450%7CMCAAMLH-1501021709%7C7%7CMCAAMB-1501021709%7CNRX38WO0n5BH8Th-nqAG_A%7CMCAID%7C2CAED21805079FB6-40000111A0014D53%7CMCIDTS%7C17359%7CMCOPTOUT-1499805353%7CNONE%7CMCSYNCSOP%7C411-17361%7CvVersion%7C2.1.0; cX_G=cx%3A12tit3mo683o6qlihfmrc4ukw%3A3nec94y8xyzg6; s_fid=285CD886C3797AF3-17AC0A56D3BE1AE4; _ncg_g_id_=5251b8e5-85ef-49ab-9cb6-895924a97ffa; GED_PLAYLIST_ACTIVITY=W3sidSI6IjU2VzUiLCJ0c2wiOjE1MDA1MjQ2NTAsIm52IjoxLCJ1cHQiOjE1MDA1MTE2MTEsImx0IjoxNTAwNTI0NjQ5fV0.; DJCOOKIE=ORC%3Dna%2Cus; s_vnum=1501905217437%26vn%3D47; s_vmonthnum=1501560000005%26vn%3D42; run-slo=true; wsjregion=na%2Cus; optimizelySegments=%7B%227731792487%22%3A%22true%22%2C%225107762377%22%3A%22gc%22%2C%225119130028%22%3A%22search%22%2C%225119130029%22%3A%22none%22%2C%228419251058%22%3A%22true%22%2C%225102690584%22%3A%22false%22%2C%228353100649%22%3A%22true%22%2C%228412721504%22%3A%22true%22%2C%228412531110%22%3A%22true%22%2C%228417010251%22%3A%22true%22%7D; optimizelyBuckets=%7B%225911560489%22%3A%225915450169%22%2C%228427202117%22%3A%228427400077%22%7D; ResponsiveConditional_initialBreakpoint=lg; utag_main=v_id:015d160820ab000a88a4e328e8dc05079003507100bd0$_sn:47$_ss:0$_st:1500530364065$vapi_domain:wsj.com$_pn:2%3Bexp-session$ses_id:1500528555427%3Bexp-session; s_monthinvisit=true; s_invisit=true; gpv_pn=WSJ_Article_Mobile_20170704_What%E2%80%99s%20News%3A%20World-Wide; s_cc=true; _ncg_id_=15d1663300f-a5411480-cc1a-4195-9514-54b54f6fc0d7; _ncg_sp_id.5378=5131c27e-1075-4df8-9b79-a5a7d53c4464.1499313219.46.1500528564.1500524928.1c89d975-ed30-4a86-bda5-5372a86ffaa9; _ncg_sp_ses.5378=*; ki_t=1499313262608%3B1500506315110%3B1500528564292%3B11%3B197; ki_r=; NaN_hash=a829ca4fRPCDRVLK1499309105519; _mibhv=anon-1499313262868-9345571041_4171; bkuuid=xmVpmR8B99eLpVPj; sc.ASP.NET_SESSIONID=vjhamv10ueqghgjzmrv4r0rp; cX_P=j4rwdnnkmbi9kliu; optimizelyPendingLogEvents=%5B%5D; s_sq=djglobal%2Cdjwsj%3D%2526pid%253DWSJ_Article_Mobile_20170704_What%2525E2%252580%252599s%252520News%25253A%252520World-Wide%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Faccounts.wsj.com%25252Flogin%25253Ftarget%25253Dhttp%2525253A%2525252F%2525252Fwww.wsj.com%2525252Farticles%2525252Fwhats-news-world-wide-14%2526ot%253DA; _parsely_slot_click={%22url%22:%22https://www.wsj.com/articles/whats-news-world-wide-1499135777%22%2C%22x%22:1279%2C%22y%22:70%2C%22xpath%22:%22//*[@id=%5C%22full-header%5C%22]/div[1]/div[1]/div[1]/header[1]/div[1]/div[1]/div[1]/div[1]/a[2]%22%2C%22href%22:%22https://accounts.wsj.com/login?target=http%253A%252F%252Fwww.wsj.com%252Farticles%252Fwhats-news-world-wide-1499135777%22}; _parsely_session={%22sid%22:45%2C%22surl%22:%22https://www.wsj.com/articles/whats-news-world-wide-1499135777%22%2C%22sref%22:%22%22%2C%22sts%22:1500528556513%2C%22slts%22:1500524406830}; _parsely_visitor={%22id%22:%2269cd7ca0-9b6c-495b-98b3-eb4cf869213d%22%2C%22session_count%22:45%2C%22last_session_ts%22:1500528556513}; run-sso=true; user_type=subscribed; TR=V2-f8b7ec452daa7484a5b670937b4cee09d7b04d672d0380fd5c0254ff895b4446; REMOTE_USER=12992a41-d64d-4490-9c0e-9c9683b5b82b; djcs_auto=M1500498224%2FsFnpfxdlmE44QdhjmXHu2S7JlHW64OygZxNuHhNx1oQuQA%2BkoUohvD2%2F6u6Y2fzqkXHB%2BxI%2BV25WyC%2BimaZRncBiQy7BKnvWUJNPoadC%2F%2FLm%2F6WXC6duIzmnihpVzj57By5HEdiUWDr3Mcv9F7PNZI2CCOlykFo22xWLxfkKFyFaXmTamJUmgYC%2BwPkNIUTSW7QvVF0TXvhN445pCwvN8JkhrkyNuupK%2BBKcKJLhEfbnSex8Tuxt%2F%2BSUds5ZiHsDE0IH6pYqsMq45C83RJjMYw%3D%3DG; djcs_sid=M1500525828%2FEdMAQYDPxeTi8hiD0SOx6ga16Tik1R750Quvpn9iv7Y3QBIetzwNb%2FoEr7KGd2R6zSJ9jXktc9ET2EbNgleM4UsHqSflLz1CQLqVe3F%2FL0A%3DG; djcs_session=M1500525828%2F3tX2Y6NBiqiU3%2FNbRSHLAuiH%2BMiENDCjK%2FAG4Q5R1ZwVm5SySYpDRIkE2bi88HrmLiiEiFHDkqTbAClyv5hadnS3GT46g2mVkkcyZdgtIWl8E71wYrX5eW8cbsgHKSn7RLtngvPF1a5HBSBNXz2rFJdw83VIr7r%2F7GddiwB5ZsLBuMrWH8btCYHpKBq2WvxllEpUjgvsMBU93f0FqjoYkqJI%2FSiyS4wnQUih7bpxTGhVALdYuGIp0YDE%2B0QScL%2BEak1uDdr2L0qbtsNypy%2BoFwiWxmjIleRqVbnSQy5LSYYLX4dQ8G10RbxYWFbqg54X23ZgD2ah6tfvVURFks767PbHG7QLrgCXpcdy9TP%2B38%2BBxkllU3iWrVfMzUFJdNcH1hUStrUHbfDSzY746PJ0FP4F%2FbloKiniUUkVEhY%2F1LixHU%2F%2F5ASNd77xorvRKFRiy02lyCo4HOoOmfSa7P3XZ5hNJc1m8HE%2Bh3FcA2gVsYY%3DG; djcs_info=eyd1c2VyJzonY3l5MjkyJTQwbnl1LmVkdScsJyN2ZXInOicxJywnbGFzdF9uYW1lJzonWWFuZycsJ3Nlc3Npb24nOidiMjMyZmJkOS1jODJkLTQ0N2MtYTNkYi0yY2ZiNTczMTIwMjcnLCdyb2xlcyc6J1dTSiUyQ1dTSi1BUkNISVZFJTJDRlJFRVJFRy1CQVNFJTJDV1NKLU1PQklMRSUyQ05QLVNFUlZJQ0UlMkNGUkVFUkVHLUlORElWSURVQUwlMkNXU0otU0VMRlNFUlYnLCd1dWlkJzonMTI5OTJhNDEtZDY0ZC00NDkwLTljMGUtOWM5NjgzYjViODJiJywnZW1haWwnOidjeXkyOTIlNDBueXUuZWR1JywnZmlyc3RfbmFtZSc6J0NodW4tWWknLCd1c2VyLXRhZ3MnOidXU0otU1VCJ30%3D",
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
                    keywords = keywordsTag['content']
                    time.sleep(1)
                    yield {'news_time':artTime, 'news_title':title, 'content':article, \
                            'keywords' : keywords, 'url':url}
                    break

                except Exception as e:
                    if try_count < 3:
                        time.sleep(5)
                        try_count +=1
                    else:
                        with open('log/wsjScrap.log', 'a') as log:
                            log.write('{0}, {1}'.format(url, str(e)))
                        break

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

if __name__ == '__main__':
    preURL = 'http://www.wsj.com/public/page/archive-'
    start_date = date(2017, 6, 7)
    end_date = date(2017, 7, 20)
    for single_date in daterange(start_date, end_date):
        with open('log/wsjScrap.log', 'a') as log:
            log.write('---{0}\n'.format(single_date.strftime('%Y-%m-%d')))
        hrefList = []
        hrefList.extend(getArchiveURLs(preURL + single_date.strftime('%Y-%-m-%d') + '.html'))

        wsjArticleList = []
        wsjArticleList.extend(getArticle(hrefList))

        with open('wsjArticle/{0}.json'.format(single_date.strftime('%Y-%m-%d')), 'w') as p:
            json.dump(wsjArticleList, p)
