#!/usr/bin/env python
# coding=utf-8

# 南方电网网站公告爬虫，记录公告名称、URL、公告日期到数据库

import urllib.request
from bs4 import BeautifulSoup
import pymysql
import re

# 代理头部信息
hdrs = {'User-Agent':'Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'}
# 默认第一页
url = 'http://www.bidding.csg.cn/dbsearch.jspx?pageNo=1&q=&org=&types='

request = urllib.request.Request(url, headers=hdrs)
# 利用urlopen获取页面代码
response = urllib.request.urlopen(request)
# 将页面转化为UTF-8编码
pageCode = response.read().decode('utf-8')

# 匹配分页条数，re.S表示匹配换行
matchObj = re.match(r'.*/(\d+)页', pageCode, re.S)
split_size = 1
if matchObj:
    # 如果检索到分页，存储分页
    split_size = int(matchObj.group(1))
else:
    print ('No match!!')

print('分页条数共计:%d' % split_size)

try:
    store_flag = False  # 是否数据库已存储标记，如果遍历到已存储公告退出
    insert_count = 0  # 本次记录插入数据库条数
    
    print('连接到mysql服务器...')
    # 打开数据库连接
    db = pymysql.connect('localhost', 'pythondb', 'pythondb', 'pythondb', charset='utf8')
    print('连接上了!')
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()

    # 逐页请求数据
    for i in range(1, split_size):
        # 访问一页后休眠2秒
        # time.sleep(2)
        url = 'http://www.bidding.csg.cn/dbsearch.jspx?pageNo=' + str(i) + '&q=&org=&types='
        print('开始提取第' + str(i) + '页')
        request = urllib.request.Request(url, headers=hdrs)
        # 利用urlopen获取页面代码
        response = urllib.request.urlopen(request)
        # 将页面转化为UTF-8编码
        pageCode = response.read().decode('utf-8')
        soup = BeautifulSoup(pageCode, 'lxml')
        # 获取分页区域的div    class="BorderEEE NoBorderTop List1 Black14 Padding5"
        web_div = soup.find(class_='BorderEEE NoBorderTop List1 Black14 Padding5')
        # 获取当前页公告列表
        web_lis = web_div.find_all('li')
        
        # 遍历列表里所有的li标签单项
        for web_li in web_lis:
            # 页面元素中提取需要的数据                                   
            ggrq = web_li.find('span').string.strip()  # 公告日期
            web_li_a = web_li.find('a')  # 公告a链接
            ggmc = web_li_a.get('title')  # 公告名称
            ggurl = web_li_a.get('href')  # 公告链接
            # 检索数据库是否已存储记录
            query_cggg = ('select count(*) from Cggg where Ggurl = %s') 
            query_data_cggg = ('http://www.bidding.csg.cn' + ggurl)
            cursor.execute(query_cggg, query_data_cggg)
            row_count = cursor.fetchone()
            if row_count[0] == 0 :
                    # 如果数据库未存储，插入公告记录到数据库 
                    insert_cggg = ('INSERT INTO CGGG(Ggmc,Ggurl,Ggrq)' 'VALUES(%s,%s,%s)')
                    data_cggg = (ggmc, 'http://www.bidding.csg.cn' + ggurl, ggrq)

                    # 执行sql语句
                    cursor.execute(insert_cggg, data_cggg)
                    # 提交到数据库
                    db.commit()
                    # 计数器加一
                    insert_count += 1
                    print('插入成功!')
            else:
                    store_flag = True
                    print('记录已存在，退出当前页循环！')
                    break
                
        # 如果出现数据记录存在情况，退出分页遍历循环
        if store_flag :
            print('数据已存储，退出分页循环！')
            break
    print('共插入记录：%d，爬取数据并插入mysql数据库完成...' % insert_count)
except Exception as e:    
        print(e)
        # 错误回滚    
        db.rollback()    
finally:    
        db.close()    
