
# coding: utf-8

# # 中债数据-统计数据
#
# Excel文件下载链接是
#
# http://www.chinabond.com.cn/DownLoadxlsx?sId=0101&sBbly=199701&sMimeType=4


from urllib.request import urlopen
import re
from lxml import html

# 协程并发任务
import logging
from tqdm import tqdm
import asyncio
import aiohttp
from contextlib import closing

logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s %(message)s')
# 年月区间
month_range = ['{0}{1:02d}'.format(y, m)
               for y in range(1997, 2017) for m in range(1, 13)]

sem = asyncio.Semaphore(8)
headers = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; WOW64)"
                          "AppleWebKit 537.36 (KHTML, like Gecko) Chrome"),
           "Accept": ("text/html,application/xhtml+xml,application/xml;"
                      "q=0.9,image/webp,*/*;q=0.8")}
# 获取文件名称和sid编号
pp = html.document_fromstring(urlopen('http://www.chinabond.com.cn/Channel/19012917').read())
table = [re.search("showapp\(.+?,'(?P<name>.+?)','(?P<sid>\d+)'\);", x.get('onclick')).groupdict()
         for x in pp.xpath('//*[@id="tabContent0"]/div[2]/ul//a')]


print('数据表如下：\n\n编号\t名称')
for x in table:
    print('{sid}\t{name}'.format(**x), end='\n')
print('开始下载')

async def wait_with_progress(tasks):
    '''
    利用tqdm显示进度条
    parameters
    ==========
    task : int
        任务列表
    '''
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        await f

async def get_async(ym, x):
    '''
    通过GET请求获取xls文件内容
    parameters
    ==========
    ym : str
        年月
    x : dict
        中文名称，编号
    '''
    with (await sem):
        name, sid = x['name'], x['sid']
        url = 'http://www.chinabond.com.cn/DownLoadxlsx?sId={}&sBbly={}&sMimeType=4'.format(
            sid, ym)
        outfilename = 'data/{}{}{}.xls'.format(sid, name, ym)
        logging.info('downloading %s', outfilename)
        response = await aiohttp.request('GET', url, headers=headers)
        with closing(response), open(outfilename, 'wb') as file:
            while True:  # 保持文件
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                file.write(chunk)
        logging.info('done %s', outfilename)

def get_all():
    '''运行asyncio loop获取所有xls数据'''
    loop = asyncio.get_event_loop()
    f = [get_async(ym, x) for ym in month_range for x in table]
    loop.run_until_complete(wait_with_progress(f))


if __name__ == '__main__':
    get_all()
