#!/usr/bin/python
# -*- coding: UTF-8 -*-

# python select io多路复用测试代码
# 1.简单的使用select来进行客户端多连接

import select
import socket
import time

# select 把socket放入 select中,然后每当有一个连接过来,把连接conn放入select模型里面去
port = 19834
ip = "127.0.0.1"

ss = socket.socket()
ss.bind((ip, port))
ss.listen(10)

readable_list = [ss]

while True:
    # print("Listen again")
    rlist, wlist, xlist = select.select(readable_list, [], [], 10)
    # 如果遍历出来的
    print('listen to the readable sockets', rlist)
    print('listen to the writeable sockets', wlist)
    print('listen to the exceptable sockets', xlist)
    print('length of the readable sockets', len(rlist))
    print('length of the total sockets', len(readable_list))
    for i in rlist:
        if i is ss:
            # 如果ss准备就绪,那么说明ss就可以接受连接了,当ss接受到连接
            # 那么把连接返回readlist
            conn, addr = i.accept()
            readable_list.append(conn)
        else:
            data = i.recv(1024)
            if not data:
                readable_list.remove(i)
            else:
                print(data.decode("gb2312"))

    for i in xlist:
        if readable_list.count(i) != 0:
            readable_list.remove(i)
            print("=========")
        i.close()
