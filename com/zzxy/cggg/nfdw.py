#!/usr/bin/env python
# coding=utf-8

# 南方电网网站公告爬虫，记录公告名称、URL、公告日期到数据库

import re
import time
import urllib.request

from bs4 import BeautifulSoup
import pymysql


# 代理头部信息
hdrs = {'User-Agent':'Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'}
# 默认第一页
url = 'http://www.sunbidding.com.cn/dbsearch.jspx?pageNo=1&q=&org=&types='

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
    max_repeat_count = 15  # 最大重复重复记录，处理因为部分公告排序前面导致数据库直接存在记录退出问题
    insert_count = 0  # 本次记录插入数据库条数
    
    print('连接到mysql服务器...')
    # 打开数据库连接
    db = pymysql.connect('localhost', 'pythondb', 'pythondb', 'pythondb', charset='utf8')
    print('连接上了!')
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()

    # 逐页请求数据
    for i in range(1, split_size):
        url = 'http://www.sunbidding.com.cn/dbsearch.jspx?pageNo=' + str(i) + '&q=&org=&types='
        print('开始提取第%d页，已插入%d条记录' %(i, insert_count))
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
            query_data_cggg = ('http://www.sunbidding.com.cn' + ggurl)
            cursor.execute(query_cggg, query_data_cggg)
            row_count = cursor.fetchone()
            if row_count[0] == 0 :
                    # 如果数据库未存储，插入公告记录到数据库 
                    insert_cggg = ('INSERT INTO CGGG(Id,Ggmc,Ggurl,Ggrq)' 'VALUES(UUID(),%s,%s,%s)')
                    data_cggg = (ggmc, 'http://www.sunbidding.com.cn' + ggurl, ggrq)

                    # 执行sql语句
                    cursor.execute(insert_cggg, data_cggg)
                    # 提交到数据库
                    db.commit()
                    # 计数器加一
                    insert_count += 1
            else:
                    # 如果最大重复数量大于0，则继续遍历下一条
                    if max_repeat_count > 0 :
                        max_repeat_count -= 1
                        continue
                    else :
                        print('达到容错上限，退出当前页记录循环！')
                        break
        # 如果重复记录超过上限，则退出分页遍历
        if max_repeat_count == 0 :
            print('达到容错上限，退出分页循环！')
            break
        
        # 访问一页后休眠2秒，避免请求过快对服务器造成压力
        time.sleep(2)
    print('共插入记录：%d\n爬取数据并插入mysql数据库完成...' % insert_count)
except Exception as e:    
        print(e)
        # 错误回滚    
        db.rollback()    
finally:    
        db.close()    
