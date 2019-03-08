#coding=utf-8
import pymssql
import time
# import hashlib
from multiprocessing import Process,Queue
import os
import threading
coding="UTF-8"

server = "192.168.2.125"    # 连接服务器地址
user = "guowen"# 连接帐号
password = "guowen"# 连接密码

conn = pymssql.connect(host = server,user = user,password = password,database = "temp_page_url")
cursor = conn.cursor()#插入数据的位置
conn2 = pymssql.connect(host = "192.168.2.114",user = "temp_parse@user",password = "temp_parse@pass",database = "Temp_Webpage_Parse")
cursor2 = conn2.cursor()#取出的数据（需要用的）
conn3 = pymssql.connect(host = server,user = user,password = password,database = "temp_page_url")
cursor3 = conn3.cursor()#用于查重的总数据

cursor2.execute("select * from fulltextLhc")#取出的数据
cursor3.execute("select urlMd5 from pages_md5")

def add_urlMd5(my_set):
    for (num,urlMd5_data) in enumerate(cursor3):
        # if num ==10000:#小范围测试
        #     break
        # print(urlMd5_data)

        if num%10000000==0:
            print("时间: {} 载入内存数量{}".format(time.strftime("%Y-%m-%d %H-%M-%S", time.localtime()),num))
        my_set.add(urlMd5_data[0])#取出单个的一列也会放在元组中，需要用下标来取
        # for i in my_set:
        #     print(i)

# min_docid = '9000014740296011'
min_docid = '9000014740295911'
def get_docid():
    global min_docid
    min_docid = "90"+ str(int("1" + min_docid[2:14]) + 1)[1:] + "11"
    return min_docid

def checkout_item(item):#put过程是单进程，放在这里可能会比较慢？
    try:
        urlMd5_test = item[0]
        url_test = item[1]
        host_test = item[2]
        content_test = item[3]
        title_test = item[4]
        if len(item[3].strip().replace(" ", "")) < 100:
            return "None"
        if content_test==" " or title_test==" ":
            return "None"
    except:
        try:
            print("urlMd5",item[0])
        except:
            print("urlMd5数据出错")
        return "None"
    return item

def data_put(q,my_set):
    # for i in my_set:
    #     print("子进程调用",i)
    print("生产者内",len(my_set))
    n = 0
    for item_data in cursor2:
        item = checkout_item(item_data)
        if item == "None":
            continue
        if item[0] not in my_set:
            # print("item[0]",item[0])
        # print(item)
        # break
            n += 1
            q.put(item)
            # print("加入队列成功")
            if n % 100000 == 0:
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

        urlMd5=item[0]
        url = item[1]
        host = item[2]
        content = item[3]
        title = item[4]
        docid = get_docid()

        # if url is None or urlMd5 is None:
        #     print('p {}; t {}; 未获得url 退出; '.format(i, j, item))
        #     return

        data = (urlMd5,url,host,content,title,docid)
        sql = "INSERT INTO fulltextLhc_new(urlMd5,url,host,content,title,docid) VALUES (%s,%s,%s,%s,%s,%s)"
        # print(data)
        # break
        try:
            cursor.execute(sql, data)
            conn.commit()
            ins += 1
        except:
            # m+=1
            # print("重复数据")
            # if m%10000 == 0:
            #     print("重复数据：",m)
            pass
        if n and n % 100000 == 0:
            print('p {}; j {}; n {}; ins {}'.format(i, j, n, ins))
def main(i, q):
    t_list = []
    for j in range(t_count):
        t = threading.Thread(target=deal, args=(i, j, q))
        t.start()
        t_list.append(t)
        time.sleep(0.3)
    for t in t_list:
        t.join()


p_count = 100#进程数
t_count = 1

if __name__ == '__main__':
    my_set = set()
    add_urlMd5(my_set)
    print("集合总长度",len(my_set))
    q = Queue(maxsize=200)
    # data_put(q)
    print("开始: {}".format(time.strftime("%Y-%m-%d %H-%M-%S", time.localtime())))
    p1 = Process(target=data_put, args=(q,my_set,))
    p1.daemon = True#p.daemon = True:  主进程运行完不会检查子进程的状态（是否执行完），直接结束进程；
                    #p.daemon = False: 主进程运行完先检查子进程的状态（是否执行完），子进程执行完后，直接结束进程；
    p1.start()
    # time.sleep(60*10)
    p_list = []
    for i in range(p_count):
        p = Process(target=main, args=(i, q))
        p.start()
        p_list.append(p)
        # time.sleep(2)
    for p in p_list:
        p.join()
    print("结束: {}".format(time.strftime("%Y-%m-%d %H-%M-%S", time.localtime())))