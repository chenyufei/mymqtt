# python select io多路复用测试代码
# 1. 简单的使用select来进行客户端多连接

import select
import socket
import threading

# select 把socket放入 select中,然后每当有一个连接过来,把连接conn放入select模型里面去

port = 19869
ip = "127.0.0.1"


def input_line(ss):
    while True:
        msg = input("input:")
        ss.send(msg.encode('utf8'))

def read_line(ss):
    while True:
        msg = ss.recv(1024)
        print('revice from',msg)

def loop():
    ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ss.connect((ip, port))
    t1 = threading.Thread(target=input_line,args=(ss,))
    t2 = threading.Thread(target=read_line, args=(ss,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

if __name__ == '__main__':
    loop()
