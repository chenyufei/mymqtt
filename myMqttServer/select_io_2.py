# python select io多路复用测试代码
# 1. 简单的使用select来进行客户端多连接

import select
import socket
import time

# select 把socket放入 select中,然后每当有一个连接过来,把连接conn放入select模型里面去
port = 19860
ip = "127.0.0.1"
ss = socket.socket()
ss.bind((ip, port))
ss.listen(10)
read_list = [ss]
write_list = []
msg_list = dict()
while 1:
    # print('listen again')
    rlist, wlist, xlist = select.select(read_list, write_list, [], 5)
    print('listen to the readable sockets', rlist)
    print('listen to the writeable sockets', wlist)
    for i in rlist:
        if i is ss:
            # 如果ss准备就绪,那么说明ss就可以接受连接了,当ss接受到连接
            # 那么把连接返回readlist
            conn, addr = i.accept()
            read_list.append(conn)
        # 如果不是socket对象,那么就是conn连接对象了,如果是conn连接对象,那么就代表有
        # 读入数据的变化,对应recv方法
        else:
            try:
                data = i.recv(1024)
                # 如果接受不到数据了 则说明连接已经关闭了
                if not data:
                    print('connecting close')
                    read_list.remove(i)
                    break
                # 我们去发送数据,但是我们要把conn准备好了再去发送
                # 所以首先把数据存在一个dict中msg_list,然后再等他准备好的时候
                # 再去发送
                msg_list[i] = [data]
                if i not in write_list:
                    write_list.append(i)
            except Exception:
                read_list.remove(i)

    for j in wlist:
        # 把对应各自的消息取出来
        msg = msg_list[j].pop()
        try:
            j.send(msg)
            # 回复完成后,一定要将outputs中该socket对象移除
            write_list.remove(j)
        except Exception:
            # 如果报错就所以连接或者已经断开了,那么我们就把他移出出去
            write_list.remove(j)
