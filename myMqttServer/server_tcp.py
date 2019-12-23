#!/usr/bin/python
# -*- coding: UTF-8 -*-
import select
import socket
import queue
from time import sleep
import os
import json

Reserved = 0  # Forbidden Reserved
CONNECT = 1  # Client to Server Connection request
CONNACK = 2  # Server to Client Connect acknowledgment
PUBLISH = 3  # Client to Server or Server to Client Publish message
PUBACK = 4  # Client to Server or Server to Client Publish acknowledgment (QoS 1)
PUBREC = 5  # Client to Server or Server to Client Publish received (QoS 2 delivery part 1)
PUBREL = 6  # Client to Server or Server to Client Publish release (QoS 2 delivery part 2)
PUBCOMP = 7  # Client to Server or Server to Client Publish complete (QoS 2 delivery part 3)
SUBSCRIBE = 8  # Client to Server Subscribe request
SUBACK = 9  # Server to Client Subscribe acknowledgment
UNSUBSCRIBE = 10  # Client to Server Unsubscribe request
UNSUBACK = 11  # Server to Client Unsubscribe acknowledgment
PINGREQ = 12  # Client to Server PING request
PINGRESP = 13  # Server to Client PING response
DISCONNECT = 14  # Client to Server or Server to Client Disconnect notification
AUTH = 15  # Client to Server or Server to Client Authentication exchange

CONNECT_ACCEPTED = 0x00
CONNECT_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION = 0x01
CONNECT_REFUSED_IDENTIFIER_REJECTED = 0x02
CONNECT_REFUSED_SERVER_UNAVAILABLE = 0x03
CONNECT_REFUSED_BAD_USER_NAME_OR_PASSWORD = 0x04
CONNECT_REFUSED_NOT_AUTHORIZED = 0x05


client_id = ""
username = ""
password = ""
config_name = []
config_password = []
sub_msg_id = 1


class SubscribePublishTopic:
    def __init__(self):
        self.__topic = ""
        self.__topic_id = 0
        self.__topic_qos = 0
        self.__payload = ""
        self.__subscribe_socket = None

    def set_topic(self, topic):
        self.__topic = topic

    def get_topic(self):
        return self.__topic

    def set_id(self, topic_id):
        self.__topic_id = topic_id

    def get_id(self):
        return self.__topic_id

    def set_qos(self, qos):
        self.__topic_qos = qos

    def get_qos(self):
        return self.__topic_qos

    def set_payload(self, payload):
        self.__payload = payload

    def get_payload(self):
        return self.__payload

    def set_subscribe_socket(self,client_socket):
        self.__subscribe_socket = client_socket

    def get_subscribe_socket(self):
        return self.__subscribe_socket


subscribe_topic = {}
publish_topic = []


def load_user_config():
    global config_name, config_password
    try:
        if os.path.exists("user.json"):
            fd = open("user.json")
            login = fd.read()
            fd.close()
            jdict = json.loads(login)
            config_name = jdict["username"]
            config_password = jdict["password"]
            return True
        else:
            return False
    except Exception as e:
        print("load_user_config Error:", e)
        return False


def mqtt_num_rem_len_bytes(buf):
    num_bytes = 1
    if (buf[1] & 0x80) == 0x80:
        num_bytes = num_bytes + 1
        if (buf[2] & 0x80) == 0x80:
            num_bytes = num_bytes + 1
            if (buf[3] & 0x80) == 0x80:
                num_bytes = num_bytes + 1
    return num_bytes


def get_msg_length(data, rem_len):
    multiplier = 1
    value = 0
    index = 0
    while True:
        if index > rem_len:
            break
        digit = int(data[index])
        index = index + 1
        value += (digit & 127) * multiplier
        multiplier *= 128
        if (digit & 128) == 0:
            return value


def connect_req(data):
    global client_id, username, password
    try:
        index = 0
        print("Message Type: Connect Command (1)")
        if fixed_head_first_byte == 0x10:
            print("Reserved: 0")
        else:
            if fixed_head_first_byte & 0x08 == 0x08:
                print("DUP flag is 1")
            elif fixed_head_first_byte & 0x06 == 0x02 or fixed_head_first_byte & 0x06 == 0x04:
                print("qos is %d" % (fixed_head_first_byte & 0x06))
            elif fixed_head_first_byte & 0x06 == 0x06:
                print("qos is Reserved")
        rem_len = mqtt_num_rem_len_bytes(data)
        msg_len = get_msg_length(data, rem_len)
        print("msg_len=%d" % msg_len)
        index = index + rem_len + 1
        protocol_name_length = int(data[index]) * 256 + int(data[index + 1])
        print("protocol_name_length: %d" % protocol_name_length)
        index = index + 2
        protocol_name = data[index:index + protocol_name_length].decode("utf-8")
        print("protocol_name: %s" % protocol_name)
        index = index + protocol_name_length
        protocol_version = int(data[index])
        print("protocol_version: %d" % protocol_version)
        index = index + 1
        connect_flag = int(data[index])
        if (connect_flag & (0x01 << 7)) == (0x01 << 7):
            print("User Name Flag is set")
        else:
            print("User Name Flag is not set")

        if (connect_flag & (0x01 << 6)) == (0x01 << 6):
            print("Password Flag is set")
        else:
            print("Password Flag is not set")

        if (connect_flag & (0x01 << 5)) == (0x01 << 5):
            print("Will Retain is set")
        else:
            print("Will Retain is not set")
        if (connect_flag >> 3) & 0x00 == 0x00:
            print("Qos Level: At most once delivery (Fire and Forge) (0)")
        elif (connect_flag >> 3) & 0x01 == 0x01:
            print("Qos Level: At least once delivery (Acknowledged and delivery) (1)")
        elif (connect_flag >> 3) & 0x02 == 0x02:
            print("Qos Level: Exactly once delivery (Assured and delivery) (2)")
        elif (connect_flag >> 3) & 0x03 == 0x03:
            print("Qos Level: Reserved (3)")

        if (connect_flag & (0x01 << 2)) == (0x01 << 2):
            print("Will Flag is set")
        else:
            print("Will Flag is not set")

        if (connect_flag & (0x01 << 1)) == (0x01 << 1):
            print("Clean Session Flag is set")
        else:
            print("Clean Session Flag is not set")

        if connect_flag == 0x01:
            print("Reserved Flag is set")
        else:
            print("Reserved Flag is not set")
        index = index + 1
        keep_alive_time = int(data[index]) * 256 + int(data[index + 1])
        print("Keep Alive: %d" % keep_alive_time)
        index = index + 2
        client_id_length = int(data[index]) * 256 + int(data[index + 1])
        print("Client ID Length: %d" % client_id_length)
        index = index + 2
        client_id = data[index:index + client_id_length].decode("utf-8")
        print("Client ID: %s" % client_id)
        index = index + client_id_length
        username_length = int(data[index]) * 256 + int(data[index + 1])
        index = index + 2
        print("User Name Length: %d" % username_length)
        username = data[index:index + username_length].decode("utf-8")
        print("User Name: %s" % username)
        index = index + username_length
        password_length = int(data[index]) * 256 + int(data[index + 1])
        index = index + 2
        print("Password Length: %d" % password_length)
        password = data[index:index + password_length].decode("utf-8")
        print("Password: %s" % password)
        index = index + password_length
        return True
    except Exception as e:
        print(e)
        return False


def resp_connect_req(client_socket):
    result = bytearray()
    result.append(0x20)
    result.append(0x02)
    result.append(0x00)
    code = CONNECT_ACCEPTED
    if not client_id.startswith("claa"):
        code = CONNECT_REFUSED_IDENTIFIER_REJECTED
    elif username in config_name:
        index = config_name.index(username)
        if index >= len(config_password):
            code = CONNECT_REFUSED_BAD_USER_NAME_OR_PASSWORD
        elif password.__eq__(config_password[index]):
            code = CONNECT_ACCEPTED
        else:
            code = CONNECT_REFUSED_BAD_USER_NAME_OR_PASSWORD
    elif username.__eq__("admin") and password.__eq__("public"):
        code = CONNECT_ACCEPTED
    else:
        code = CONNECT_REFUSED_BAD_USER_NAME_OR_PASSWORD
    result.append(code)
    client_socket.send(result)
    return code


def ping_resp(client_socket):
    result = bytearray()
    result.append(0xd0)
    result.append(0x00)
    client_socket.send(result)


def subscribe_req(data, client_socket):
    try:
        index = 0
        rem_len = mqtt_num_rem_len_bytes(data)
        msg_len = get_msg_length(data, rem_len)
        index = index + rem_len + 1
        msg_id = int(data[index]) * 256 + int(data[index+1])
        index = index + 2
        topic_length = int(data[index]) * 256 + int(data[index+1])
        index = index + 2
        topic = data[index:index+topic_length].decode("utf-8")
        index = index + topic_length
        qos = int(data[index])
        index = index + 1
        print("%s subscribe topic:%s,qos=%d" % (client_socket.getpeername()[0], topic, qos))
        subTopic = SubscribePublishTopic()
        subTopic.set_id(msg_id)
        subTopic.set_qos(qos)
        subTopic.set_topic(topic)
        subTopic.set_subscribe_socket(client_socket)
        if topic in subscribe_topic.keys():
            tempTopic = subscribe_topic[topic]
            for item in tempTopic:
                if item.get_topic == topic and item.get_subscribe_socket() == client_socket:
                    tempTopic.remove(item)
                    break
            tempTopic.append(subTopic)
            subscribe_topic[topic] = tempTopic
        else:
            subscribe_topic[topic] = [subTopic]
        return True, msg_id, qos
    except Exception as e:
        print(e)
        return False, None, None


def subscribe_ack(client_socket, msg_id, qos):
    result = bytearray()
    result.append(0x90)
    result.append(0x03)

    id = msg_id & 0xFFFF
    hvalue = id >> 8
    lvalue = id & 0xFF
    result.append(hvalue)
    result.append(lvalue)
    result.append(qos)
    client_socket.send(result)


def unsubscribe(data, client_socket):
    try:
        index = 0
        rem_len = mqtt_num_rem_len_bytes(data)
        msg_len = get_msg_length(data, rem_len)
        index = index + rem_len + 1
        msg_id = int(data[index]) * 256 + int(data[index + 1])
        index = index + 2
        topic_length = int(data[index]) * 256 + int(data[index + 1])
        index = index + 2
        topic = data[index:index + topic_length].decode("utf-8")
        index = index + topic_length
        print("%s unSubscribe topic:%s," % (client_socket.getpeername()[0], topic))

        if topic in subscribe_topic.keys():
            tempTopic = subscribe_topic[topic]
            for item in tempTopic:
                if item.get_topic() == topic and item.get_subscribe_socket() == client_socket:
                    tempTopic.remove(item)
                    break
            if 0 == len(tempTopic):
                del subscribe_topic[topic]
            else:
                subscribe_topic[topic] = tempTopic
        return True, msg_id
    except Exception as e:
        print(e)
        return False, None


def unsubscribe_ack(client_socket, msg_id):
    result = bytearray()
    result.append(0xb0)
    result.append(0x02)

    id = msg_id & 0xFFFF
    hvalue = id >> 8
    lvalue = id & 0xFF
    result.append(hvalue)
    result.append(lvalue)

    client_socket.send(result)


def get_bytearray(value):
    result = bytearray()
    length = (len(value)) & 0xFFFF
    hvalue = length >> 8
    lvalue = length & 0xFF
    result.append(hvalue)
    result.append(lvalue)
    return result + bytearray(bytes(value, encoding="utf8"))


def publish_message_depacketize(topic, content, qos):
    topic_bytes = get_bytearray(topic)
    content_bytes = bytearray(bytes(content, encoding="utf8"))
    result = bytearray()
    result = topic_bytes
    if 1 == qos or 2 == qos:
        global sub_msg_id
        length = sub_msg_id & 0xFFFF
        sub_msg_id = sub_msg_id + 1
        hvalue = length >> 8
        lvalue = length & 0xFF
        result.append(hvalue)
        result.append(lvalue)
    result = result + content_bytes

    data = bytearray()
    length = len(result)
    if 0 == qos:
        data.append(0x30)
    elif 1 == qos:
        data.append(0x30 | 0x02)
    elif 2 == qos:
        data.append(0x30 | 0x04)
    data.append(length)
    msg = data + result
    return msg


def start_publish():
    for topic in publish_topic:
        publish_topic.remove(topic)
        print("public topic name: %s,payload:%s" % (topic.get_topic(), topic.get_payload()))
        if topic.get_topic() not in subscribe_topic.keys():
            print("no body subscribe topic: %s" % (topic.get_topic()))
            continue
        sub_client_list = subscribe_topic[topic.get_topic()]
        for item in sub_client_list:
            client_socket = item.get_subscribe_socket()
            payload = topic.get_payload()
            qos = item.get_qos()
            msg = publish_message_depacketize(item.get_topic(), payload, qos)
            try:
                client_socket.send(msg)
            except Exception as e:
                print(e)
                client_socket.close()
                sub_client_list.remove(item)
        if 0 == len(sub_client_list):
            del subscribe_topic[topic.get_topic()]
        else:
            subscribe_topic[topic.get_topic()] = sub_client_list


def publish_req(data, client_socket):
    print("Message Type: Publish Message (3)")
    try:
        fixed_head_first_byte = int(data[0])
        if fixed_head_first_byte & 0x08 == 0x08:
            print("DUP Flag : set")
        else:
            print("DUP Flag: Not set")
        qos = 0
        if fixed_head_first_byte & 0x06 == 0x00:
            qos = 0
            print("QoS Level: At most once delivery (Fire and Forget) (0)")
        elif fixed_head_first_byte & 0x02 == 0x02:
            qos = 1
            print("Qos Level: At least once delivery (Acknowledged and delivery) (1)")
        elif fixed_head_first_byte & 0x04 == 0x04:
            qos = 2
            print("Qos Level: Exactly once delivery (Assured and delivery) (2)")
        elif fixed_head_first_byte & 0x06 == 0x06:
            qos = 3
            print("QoS Level: Reserved (3)")

        if fixed_head_first_byte & 0x01 == 0x01:
            print("Retain: set")
        else:
            print("Retain: Not set")
        remain_length = mqtt_num_rem_len_bytes(data)
        length = get_msg_length(data, remain_length)
        index = 0
        index = index + 1 + remain_length
        topic_length = int(data[index]) * 256 + int(data[index + 1])
        index = index + 2
        topic_name = data[index:index + topic_length].decode("utf-8")
        index = index + topic_length
        if 1 == qos or 2 == qos:
            msg_id = int(data[index]) * 256 + int(data[index + 1])
            print("msg id: %d" % msg_id)
            index = index + 2
        payload = data[index:length + remain_length + 1].decode("utf-8")
        if 0 == len(payload):
            print("payload msg is missing")
        else:
            print("payload msg: %s" % payload)
        index = index + len(payload)
        topic = SubscribePublishTopic()
        if 1 == qos or 2 == qos:
            topic.set_id(msg_id)
        topic.set_topic(topic_name)
        topic.set_qos(qos)
        topic.set_payload(payload)
        publish_topic.append(topic)
        if 0 == qos:
            start_publish()
            return True, index
        elif 1 == qos or 2 == qos:
            if 1 == qos:
                publish_ack(client_socket, msg_id)
                start_publish()
            elif 2 == qos:
                publish_rec(client_socket, msg_id)
            return True, index
    except Exception as e:
        print(e)
        return False, 0


def publish_ack(client_socket, msgid):
    result = bytearray()
    result.append(0x40)
    result.append(0x02)

    id = msgid & 0xFFFF
    hvalue = id >> 8
    lvalue = id & 0xFF
    result.append(hvalue)
    result.append(lvalue)

    client_socket.send(result)


# 发布QoS为2时，broke响应已收到到客户端发布的消息
def publish_rec(client_socket,msgid):
    result = bytearray()
    result.append(0x50)
    result.append(0x02)

    id = msgid & 0xFFFF
    hvalue = id >> 8
    lvalue = id & 0xFF
    result.append(hvalue)
    result.append(lvalue)

    client_socket.send(result)


def publish_rel(data, client_socket):
    try:
        print("Message Type: Publish Release (6)")
        index = 1
        remain_length = mqtt_num_rem_len_bytes(data)
        length = get_msg_length(data, remain_length)
        index = index + 1
        msgid = int(data[index]) * 256 + int(data[index+1])
        print("msg id: %d" % msgid)
        index = index + 2
        publish_comp(client_socket,msgid)
        return True, index
    except Exception as e:
        print(e)
        return False, 0


def publish_comp(client_socket, msgid):
    result = bytearray()
    result.append(0x70)
    result.append(0x02)

    id = msgid & 0xFFFF
    hvalue = id >> 8
    lvalue = id & 0xFF
    result.append(hvalue)
    result.append(lvalue)

    client_socket.send(result)


load_user_config()

# Create a TCP/IP
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setblocking(False)

# Bind the socket to the port
server_address = ('100.0.0.3', 1883)
print ('starting up on %s port %s' % server_address)
server.bind(server_address)

# Listen for incoming connections
server.listen(5)

# Sockets from which we expect to read
inputs = [server]

# Sockets to which we expect to write
# 处理要发送的消息
outputs = []

# Outgoing message queues (socket: Queue)
message_queues = {}


def remove_socket(client_socket, outputs):
    if client_socket in outputs:
        outputs.remove(client_socket)


while inputs:
    # Wait for at least one of the sockets to be ready for processing
    # print ('waiting for the next event')
    # 开始select 监听, 对input_list 中的服务器端server 进行监听
    # 一旦调用socket的send, recv函数，将会再次调用此模块
    # readable, writable, exceptional = select.select(inputs, outputs, inputs, 10)
    for soc in inputs:
        if soc.fileno() == -1:
            inputs.remove(soc)
    readable, writable, exceptional = select.select(inputs, [], inputs, 10)
    # Handle inputs
    # 循环判断是否有客户端连接进来, 当有客户端连接进来时select 将触发
    for s in readable:
        # 判断当前触发的是不是服务端对象, 当触发的对象是服务端对象时,说明有新客户端连接进来了
        # 表示有新用户来连接
        if s is server:
            # A "readable" socket is ready to accept a connection
            connection, client_address = s.accept()
            print('connection from', client_address)
            # this is connection not server
            connection.setblocking(0)
            # 将客户端对象也加入到监听的列表中, 当客户端发送消息时 select 将触发
            inputs.append(connection)

            # Give the connection a queue for data we want to send
            # 为连接的客户端单独创建一个消息队列，用来保存客户端发送的消息
            message_queues[connection] = queue.Queue()
        else:
            # 有老用户发消息, 处理接受
            # 由于客户端连接进来时服务端接收客户端连接请求，将客户端加入到了监听列表中(input_list), 客户端发送消息将触发
            # 所以判断是否是客户端对象触发
            try:
                data = s.recv(1024)
            except Exception as e:
                print(e)
                s.close()
                remove_socket(s, outputs)
                remove_socket(s, writable)
                continue
            # 客户端未断开
            if 0 != len(data):
                # A readable client socket has data
                # print('received "%s" from %s' % (data, s.getpeername()))
                fixed_head_first_byte = int(data[0])
                fixed_head_first_byte = fixed_head_first_byte >> 4
                if fixed_head_first_byte == CONNECT:
                    if connect_req(data):
                        code = resp_connect_req(s)
                        if code != CONNECT_ACCEPTED:
                            s.close()
                            remove_socket(s, outputs)
                            remove_socket(s, inputs)
                            continue
                    else:
                        s.close()
                        remove_socket(s, outputs)
                        continue
                elif fixed_head_first_byte == PINGREQ:
                    ping_resp(s)
                elif fixed_head_first_byte == SUBSCRIBE:
                    res, msgid, qos = subscribe_req(data, s)
                    if res:
                        subscribe_ack(s, msgid, qos)
                    else:
                        s.close()
                        remove_socket(s, outputs)
                        continue
                elif fixed_head_first_byte == UNSUBSCRIBE:
                    resp, msgid = unsubscribe(data, s)
                    if resp:
                        unsubscribe_ack(s, msgid)
                    else:
                        s.close()
                        remove_socket(s, outputs)
                        continue
                elif fixed_head_first_byte == PUBLISH:
                    publish_req(data, s)
                elif fixed_head_first_byte == PUBREL:
                    publish_rel(data, s)
                    start_publish()
                # 将收到的消息放入到相对应的socket客户端的消息队列中
                message_queues[s].put(data)
                # Add output channel for response
                # 将需要进行回复操作socket放到output 列表中, 让select监听
                if s not in outputs:
                    outputs.append(s)
            else:
                # 客户端断开了连接, 将客户端的监听从input列表中移除
                # Interpret empty result as closed connection
                print('closing', client_address)
                # Stop listening for input on the connection
                remove_socket(s, outputs)
                inputs.remove(s)
                s.close()

                # Remove message queue
                # 移除对应socket客户端对象的消息队列
                del message_queues[s]
    '''
    # Handle outputs
    # 如果现在没有客户端请求, 也没有客户端发送消息时, 开始对发送消息列表进行处理, 是否需要发送消息
    # 存储哪个客户端发送过消息
    for s in writable:
        try:
            # 如果消息队列中有消息,从消息队列中获取要发送的消息
            message_queue = message_queues.get(s)
            send_data = ''
            if message_queue is not None:
                send_data = message_queue.get_nowait()
            else:
                # 客户端连接断开了
                print("has closed ")
        except queue.Empty:
            # 客户端连接断开了
            # print("%s" % (s.getpeername()))
            outputs.remove(s)
        else:
            # print "sending %s to %s " % (send_data, s.getpeername)
            print("send something")
            if message_queue is not None:
                pass
                # s.send(send_data)
            else:
                print("has closed ")
            # del message_queues[s]
            # writable.remove(s)
            # print "Client %s disconnected" % (client_address)
    '''
    # Handle "exceptional conditions"
    # 处理异常的情况
    for s in exceptional:
        print('exception condition on', s.getpeername())
        # Stop listening for input on the connection
        inputs.remove(s)
        remove_socket(s, outputs)
        s.close()

        # Remove message queue
        del message_queues[s]

    # sleep(1)
