import gevent
from gevent import monkey
monkey.patch_all()

import json
import requests
from bs4 import BeautifulSoup as BS
from datetime import date
import time


TARGETS = "shrank.json"

with open(TARGETS, "r") as tf:
    ranking = json.load(tf)
subs = [ele['i'] for ele in ranking]
print(len(subs))

class Timer():
    def __init__(self, mark="default", report=False):
        self.time = None
        self.mark = mark
        self.report = report
    def reset(self):
        self.time = time.time()
    def elapsed(self, remark=''):
        cur_time = time.time()
        if self.report:
            print("[{}] time elapsed: {} {}".format(self.mark, cur_time-self.time, remark))


def fetchPage(url):
    TIMEOUT = .5
    HDR = {
        'User-agent' : 'neutrinoliu/comments_spider'
    }
    try:
        res = requests.get(url=url, headers=HDR, timeout=TIMEOUT)
        if res.status_code == 503:
            return None
    except:
        return None
    return res

class State():
    def __init__(self, id):
        self.id = id
        self.fails = []
        self.cms = []
    def resetFails(self):
        self.fails = []
    def resetCms(self):
        self.cms = []
    def appendFails(self, url):
        self.fails.append(url)
    def appendCm(self, cm):
        self.cms.append(cm)
    def getFails(self):
        return self.fails
    def getCms(self):
        return self.cms

def spider(url, myState: State):
    res = fetchPage(url)
    if res: # when 200
        return parse(res, myState) # early stop if page has no tr.topic element
    else:
        # refetch if fails
        myState.appendFails(url)
        return True

def parse(res, myState):
    def todayDate():
        return date.today().strftime("20%y-%m-%d")
    def getDate(raw):
        if 'ago' in raw:
            return todayDate()
        ymd = raw.split(" ")[1]
        y = ymd.split('-')[0]
        m = ymd.split('-')[1]
        if int(m) < 10:
            m = '0' + m
        d = ymd.split('-')[2]
        if int(d) < 10:
            d = '0' + d
        return '-'.join([y,m,d])

        
    def getRate(ele):
        if not ele:
            return '0'
        stars = ele.attrs['class'][1].replace('stars', '')
        return stars
    dom = BS(res.content,features="html.parser")
    box = dom.find(id='comment_box')
    if box == None or len(box) == 0:
        # print("no comment box element")
        return False
    items = box.select('div.item')
    for item in items:
        sid = myState.id
        uid = item.attrs['data-item-user']
        timeText = item.find('small', class_='grey').text
        ratingEle = item.find('span', class_='starlight')
        content = item.find('p').text.replace('\n', '')
        cm = [str(uid), getDate(timeText), getRate(ratingEle), content]
        myState.appendCm(cm)

    return True

def dumpFile(sid, cms):
    FILENAME = "comments/{}.txt".format(sid)
    with open(FILENAME, "a") as cf:
        for cm in cms:
            cf.write("{}\n".format("\t".join(cm)))

def dumpCms(id):
    myTimer = Timer(str(id), report=True)
    myTimer.reset()
    URL = 'https://bgm.tv/subject/{}/comments?page='.format(id)
    MAX_PAGE = 500
    CONCURRENT = 5
    all_pages = [URL + str(pid) for pid in range(MAX_PAGE)]
    myState = State(id)
    max_pg = 0
    while len(all_pages) > 0:
        myState.resetFails()

        nIters = int(len(all_pages)/CONCURRENT) + 1
        for gId in range(nIters):
            jobs = [gevent.spawn(spider, url, myState) for url in all_pages[gId * CONCURRENT: min(len(all_pages), (gId+1) * CONCURRENT)]]
            gevent.joinall(jobs)

            dumpFile(id, myState.getCms())
            myState.resetCms()

            continue_flag = True
            for j in jobs:
                continue_flag = continue_flag and j.value
            if not continue_flag:
                break
        max_pg = max(max_pg, gId * CONCURRENT)
        
        all_pages = myState.getFails()
    myTimer.elapsed(' for {} pages'.format(max_pg))
for i in subs:
    dumpCms(i)