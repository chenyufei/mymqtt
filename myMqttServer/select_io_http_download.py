# 使用socket获取http报文
# 把setblocking设为false 让学员自己解决问题
import socket
from urllib.parse import urlparse
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE

# 申明select
sel = DefaultSelector()


class Downloader:

    def __init__(self, sel):
        self.domain = ""
        self.url = ""
        self.path = ""
        self.host = ""
        self.selector = sel
        self.ss = ""
        self.data = b""

    def get_client(self):
        # 初始化socket

        self.ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 使用非阻塞io，socket不等待连接建立好 直接向下执行
        self.ss.setblocking(False)
        # 虽然申明了一个非阻塞操作,但这个还是阻塞操作,不能交给select
        try:
            self.ss.connect((self.host, 18001))
        except Exception:
            pass

        # 创建好链接之后,向服务器发送http报文,不过这个使用回调的方式来做，这个是一个写操作,因为我们
        # 向socket中写入数据(发送数据)
        self.selector.register(self.ss, EVENT_WRITE, self.send_data)

    def send_data(self, mask):
        # 连接创建好之后去请求http
        self.selector.unregister(self.ss)
        data = "GET {} HTTP/1.1\r\nHost:{}\r\nConnection:close\r\nUser-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36\r\n\r\n".format(
            self.path, self.host).encode('utf8')
        self.ss.send(data)
        # 数据发送之后,去获取http数据,这个时候使用读操作
        self.selector.register(self.ss, EVENT_READ, self.get_http)

    def get_http(self, mask):

        # 在报文没有读取完之前,循环会一直调用这个函数,所以我们不用去
        # 担心while循环,只需要在他没有报文的时候,结束这个事件就ok了
        d = self.ss.recv(1024)
        if d:
            self.data += d
        else:
            self.selector.unregister(self.ss)
            print(self.data)

    def get_html(self, domain="https://book.douban.com/subject/3012360/"):

        self.domain = domain
        self.url = urlparse(self.domain)
        self.host = self.url.netloc
        self.path = self.url.path
        if self.path == "":
            self.path = "/"

        self.get_client()


url = 'http://127.0.0.1/index1/'
# 试着同时获取30条数据
for i in range(30):
    download = Downloader(sel)
    download.get_html(url)
while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key)
