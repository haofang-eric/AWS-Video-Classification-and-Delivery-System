# coding: utf-8
import json
import os


import urllib2
from pytube import YouTube
from lxml import etree

from s3_helper import Bucket
from sqs_helper import SQS

maxNumber = 2
timeThreshold = 600
cur_count = 0
videoLists = []


def getHtml(url):
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.13 (KHTML, like Gecko) Chrome/24.0.1284.0 Safari/537.13'
    headers = {'User-Agent': user_agent}
    request = urllib2.Request(url, headers=headers)
    response = urllib2.urlopen(request)
    html = response.read()
    return html


def getUrl(html):
    global maxNumber
    global timeThreshold
    global cur_count
    global videoLists
    tree = etree.HTML(html)
    urllist = tree.xpath(u'//div[@class="thumb-wrapper"]/a/@href')
    urllist_time = tree.xpath(u'//div[@class="thumb-wrapper"]/a/span/span/text()')
    baseurl = r'https://www.youtube.com'
    for (item_name, item_length) in zip(urllist, urllist_time):
        try:
            yt = YouTube(baseurl + item_name)
            if not checktime(item_length):
                continue
            video = yt.streams.filter(progressive=True, file_extension='mp4').desc().first().download("./downloads")
            print video
            bb = Bucket()
            bb.upload_large(video)
        except Exception as e:
            print e
    #if urllist:
        #getUrl(baseurl + urllist[0])  # 下一个页面


def checktime(timelength):
    global timeThreshold
    strs = timelength.split(':')
    time = int(strs[0]) * 60 + int(strs[1])
    if time < timeThreshold:
        return True
    else:
        return False


def crawl_main():
    start_urls = [
        "https://www.youtube.com/watch?v=eJEm3pKb_Fs",
        "https://www.youtube.com/watch?v=tplHcZlgDyk"
    ]

    for item in start_urls:
        html = getHtml(item)
        getUrl(html)


if __name__ == "__main__":
    try:
        crawl_main()
    except Exception as e:
        print e
