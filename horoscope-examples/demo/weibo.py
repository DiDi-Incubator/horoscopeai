# -*- coding: utf-8 -*-
import json
import requests
from bs4 import BeautifulSoup
import datetime
import os
import argparse
import requests
import re

header = { "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
           "Accept-Encoding": "gzip, deflate, br",
           "Accept-Language": "zh-CN,zh;q=0.9",
           "Cookie": "",
           "Referer": "https://s.weibo.com/weibo?q=%E6%B3%8A%E5%AF%93&wvr=6&Refer=SWeibo_box",
           "Sec-Fetch-Mode": "navigate",
           "Sec-Fetch-Site": "none",
           "Sec-Fetch-User": "?1",
           "Upgrade-Insecure-Requests": "1",
           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36"}

def get_page(keywords, num, start_date , end_date):
    """Construct request URL.

    According to keywords , page num, start-date and end-date to construct a request URL.

    Args:
        keywords: According to keyword to search content.
        num: A page number to search content.
        start_date: From start time to search content.
        end_date: Until end date to search content.

    Returns:
        A URL and previous start time to send a request and provide a file name. For examples:

        URL: https://s.weibo.com/weibo?q='平安银行'&Refer=SWeibo_box&page=1&&timescope=custom:2022-01-01-05:2022-01-01-06
        Previous Time: 2022010105
    """
    original_url="https://s.weibo.com/weibo?q={}&Refer=SWeibo_box&page={}&&timescope=custom:{}:{}"
    t1 = datetime.datetime.strptime(start_date, "%Y-%m-%d-%H")
    t2 = datetime.datetime.strptime(end_date, "%Y-%m-%d-%H")
    delta_time = int((t2 - t1).seconds / 3600)
    for i in range(1, delta_time + 1):
        pre = t1.strftime('%Y-%m-%d-%H')
        pos = (t1 + datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')
        t1 = t1 + datetime.timedelta(hours=1)
        for page in range(1,num+1):
            url=original_url.format(keywords, page, pre, pos)
            yield url, pre


def get_content(url, start_date):
    """Get page content from URL.

    According to URL send a request to analysis the page content and get specified content.
    Write the contents to the files and horoscope will read the contents of files.

    Args:
        url: https://s.weibo.com/weibo?q='平安银行'&Refer=SWeibo_box&page=1&&timescope=custom:2022-01-01-05:2022-01-01-06
        start_date Time: 2022010105

    Returns:
        Parse the content from the web page and write it to a file. For examples:

        filename:demo2022-03-17-19.txt
    """
    response = requests.get(url, headers=header, timeout=10)
    soup = BeautifulSoup(response.text, 'lxml')
    contents = soup.findAll("div", class_="card-feed")
    f = open(os.getcwd() + "/demo/comments/demo" + start_date + ".txt", "wb+")
    reg = r'[\u4e00-\u9fa5]'
    for content in contents:
        author = content.find("a", class_="name").get_text()
        comment = content.find("p", class_="txt").get_text().strip().replace("展开全文c", "")
        date_time = content.find("p", class_="from").find("a").get_text().strip()
        comment_ch = re.findall(reg, comment)
        comment_res = ''.join(comment_ch)
        json_data = json.dumps({'text': comment_res, 'date_time':date_time}) + "\n"
        f.write(json_data.encode("utf-8"))
    f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(usage="it's usage tip.", description="help info.")
    parser.add_argument("--keyword", default='平安银行', help="请输入关键字.")
    parser.add_argument("--num", default=1, help="请输入查询页数.")
    parser.add_argument("--start_date", default='2022-03-17-19', help="请输入查询开始时间.")
    parser.add_argument("--end_date", default='2022-03-18-15', help="请输入查询开始时间.")
    parser.add_argument("--cookie", default="", help="请输入cookie" )
    args = parser.parse_args()
    header['Cookie'] = args.cookie
for link, start_date in get_page(args.keyword, args.num, args.start_date, args.end_date):
    get_content(link, start_date)
