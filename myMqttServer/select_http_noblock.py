# 使用socket获取http报文
# 把setblocking设为false 让学员自己解决问题
import socket
from urllib.parse import urlparse

# 初始化socket
domain = "https://book.douban.com/subject/3012360/"
url = urlparse(domain)
host = url.netloc
path = url.path
if path == "":
    path = "/"
ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#使用非阻塞io，socket不等待连接建立好 直接向下执行
ss.setblocking(False)
#只有一直try获取连接,因为连接还没有准备好
while True:
    try:
        ss.connect((host, 80))
    except BlockingIOError as e:
        pass
    except OSError:
        break

#'\r'的本意是回到行首，'\n'的本意是换行
data = "GET {} HTTP/1.1\r\nHost:{}\r\nConnection:close\r\nUser-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36\r\n\r\n".format(
    path, host).encode('utf8')
ss.send(data)
res = b""
#报文就不像聊天数据那么小了 需要循环的拼接报文
#让系统不断遍历 获取到recv因为不是阻塞的
while True:
    try:
        d = ss.recv(1024)
    except BlockingIOError:
        continue
    if d:
        res += d
    else:
        break
print(res.decode('utf8'))
ss.close()
