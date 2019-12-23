import select
import socket
from urllib.parse import urlparse


#
class Crawler:
    def __init__(self):
        self.socket_list = []
        self.read_list = list()
        self.write_list = list()
        self.exec_list = list()
        self.req_info = dict()
        self.msg_list = dict()
    #不断的事件循环
    def loop(self):
        while True:
            #
            rlist, wlist, xlist = select.select(self.read_list, self.write_list, self.exec_list)
            for i in wlist:
                # 侦测到写事件 就去发送报文
                host, path = self.req_info[i]
                data = "GET {} HTTP/1.1\r\nHost:{}\r\nConnection:close\r\nUser-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36\r\n\r\n".format(
                    path, host).encode('utf8')
                i.send(data)
                self.write_list.remove(i)
                self.read_list.append(i)
                #侦测到读时间就去获取报文
            for i in rlist:
                msg = i.recv(1024)
                if i not in self.msg_list.keys():
                    self.msg_list[i] = msg
                else:
                    self.msg_list[i] += msg
                    #考虑到网页的大小 一次不可能接受完毕 所以当没有数据的时候才关闭连接
                if not msg:
                    self.read_list.remove(i)
                    print(self.msg_list[i].decode('utf8'))
                    del (self.msg_list[i])

    # 初始化http请求 向类里面不断的添加新的http需求,然后初始化好socket对象 添加给时间循环
    def add_url(self, url):
        # 初始化socket请求
        url_dict = urlparse(url)
        host = url_dict.netloc
        path = url_dict.path
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect((host, 80))
        ss.setblocking(False)
        self.write_list.append(ss)
        # 把socket加入socket
        self.socket_list.append(ss)
        self.req_info[ss] = (host, path)


if __name__ == '__main__':
    crawler = Crawler()
    for i in range(100):
        crawler.add_url('https://www.cnblogs.com/myyan/p/7149542.html')
    crawler.loop()
