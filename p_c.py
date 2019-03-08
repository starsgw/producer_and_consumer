import pymssql
import time
import hashlib
from multiprocessing import Process,Queue
import psutil
import os
import threading

server = '192.168.8.213\SQL2016'    # 连接服务器地址
user = "aaa"# 连接帐号
password = "aaa"# 连接密码
database = "temp_page_url"

conn = pymssql.connect(host = server,user = user,password = password,database = database)
cursor = conn.cursor()
conn2 = pymssql.connect(host = server,user = user,password = password,database = database)
cursor2 = conn2.cursor()

cursor.execute("select * from pages")
info = psutil.virtual_memory()

def get_md5(url):
    m = hashlib.md5()
    m.update(url.encode("UTF-8"))
    md5 = m.hexdigest()
    return md5


def get_docid(item):
    try:
        docid = item[0]
    except:
        docid = None
    return docid


def get_url(item):
    try:
        url = item[1]
    except:
        url = item[1]
    return url

def data_put(q):
    n = 0
    for item in cursor:
        # print(item)
        # break
        n += 1
        q.put(item)
        if n % 10000 == 0:
            print('生产者P 0; 生产数据 {}, 队列数据 {}'.format(n, q.qsize()))
    print('生产者P 0; 共生产 {} 条数据'.format(n))
    for _ in range(1000):
        q.put(-1)




def deal(i, j, q):
    n = 0
    ins = 0
    while 1:
        if q.qsize() == 0:
            time.sleep(0.1)
            continue
        item = q.get()
        if item == -1:
            break
        n += 1
        url = get_url(item)
        docid = get_docid(item)
        url_md5 = get_md5(url)
        # print(item)
        # print(docid ,  url  , url_md5)
        # sql = "INSERT INTO pages(url,urlMd5,docid) VALUES (%s,%s,%s)"
        # data = (url, url_md5, docid)
        if url is None or url_md5 is None:
            print('p {}; t {}; 未获得url 退出; '.format(i, j, item))
            return

        data = (url, url_md5, docid)
        sql = "INSERT INTO pages_all_test(url,urlMd5,docid) VALUES (%s,%s,%s)"
        # print(data)
        # break
        try:
            cursor2.execute(sql, data)
            conn2.commit()
            ins += 1
        except:
            print("重复数据")
            pass
        if n and n % 10000 == 0:
            print('p {}; j {}; n {}; ins {}'.format(i, j, n, ins))
def main(i, q):
    t_list = []
    for j in range(t_count):
        t = threading.Thread(target=deal, args=(i, j, q,))
        t.start()
        t_list.append(t)
        time.sleep(0.3)
    for t in t_list:
        t.join()


p_count = 3
t_count = 1

if __name__ == '__main__':
    q = Queue(maxsize=5)
    # data_put(q)
    print("开始: {}".format(time.strftime("%Y-%m-%d %H-%M-%S", time.localtime())))
    p1 = Process(target=data_put, args=(q,))
    p1.daemon = True
    p1.start()
    # time.sleep(60*10)
    p_list = []
    for i in range(p_count):
        p = Process(target=main, args=(i, q,))
        p.start()
        p_list.append(p)
        # time.sleep(2)
    for p in p_list:
        p.join()
    print("结束: {}".format(time.strftime("%Y-%m-%d %H-%M-%S", time.localtime())))

















