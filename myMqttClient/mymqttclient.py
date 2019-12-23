#!/usr/bin/python
# -*- coding: UTF-8 -*-

import socket
import threading
import json
import os
import time
import sys
import base64
import binascii
import const

global_username = ''
global_password = ''
global_client_id = ''
global_ip = ''
global_port = 0

client = object()  # socket.socket(socket.AF_INET, socket.SOCK_STREAM)
const.Reserved = 0  # Forbidden Reserved
const.CONNECT = 1  # Client to Server Connection request
const.CONNACK = 2  # Server to Client Connect acknowledgment
const.PUBLISH = 3  # Client to Server or Server to Client Publish message
const.PUBACK = 4  # Client to Server or Server to Client Publish acknowledgment (QoS 1)
const.PUBREC = 5  # Client to Server or Server to Client Publish received (QoS 2 delivery part 1)
const.PUBREL = 6  # Client to Server or Server to Client Publish release (QoS 2 delivery part 2)
const.PUBCOMP = 7  # Client to Server or Server to Client Publish complete (QoS 2 delivery part 3)
const.SUBSCRIBE = 8  # Client to Server Subscribe request
const.SUBACK = 9  # Server to Client Subscribe acknowledgment
const.UNSUBSCRIBE = 10  # Client to Server Unsubscribe request
const.UNSUBACK = 11  # Server to Client Unsubscribe acknowledgment
const.PINGREQ = 12  # Client to Server PING request
const.PINGRESP = 13  # Server to Client PING response
const.DISCONNECT = 14  # Client to Server or Server to Client Disconnect notification
const.AUTH = 15  # Client to Server or Server to Client Authentication exchange

subscribe_msg_dict = {}
sub_msg_id = 1
MQTT_DUP_FLAG = 1 << 3
MQTT_QOS0_FLAG = 0 << 1
MQTT_QOS1_FLAG = 1 << 1
MQTT_QOS2_FLAG = 2 << 1

MQTT_RETAIN_FLAG = 1

MQTT_CLEAN_SESSION = 1 << 1
MQTT_WILL_FLAG = 1 << 2
MQTT_WILL_RETAIN = 1 << 5
MQTT_USERNAME_FLAG = 1 << 7
MQTT_PASSWORD_FLAG = 1 << 6


class TOPIC:
    def __init__(self):
        self.__topic = ""
        self.__topic_id = 0
        self.__topic_qos = 0
        self.__payload = ""

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


def mqtt_num_rem_len_bytes(buf):
    num_bytes = 1
    if (buf[1] & 0x80) == 0x80:
        num_bytes = num_bytes + 1
        if (buf[2] & 0x80) == 0x80:
            num_bytes = num_bytes + 1
            if (buf[3] & 0x80) == 0x80:
                num_bytes = num_bytes + 1
    return num_bytes


def calc_rem_bytes(topic_len, payload_len):
    length = 2 + topic_len + payload_len
    result = bytearray()
    while True:
        digit = length % 128
        length = length // 128
        if length > 0:
            digit = digit | 0x80
            result.append(digit)
        else:
            result.append(digit)
            break
    return result


def get_input_topic():
    while True:
        topic = input("请输入主题:\n")
        topic = topic.strip()
        if len(topic) > 0:
            return topic
        else:
            print("主题为空")


def get_input_payload():
    while True:
        content = input("请输入发布的内容:\n")
        content = content.strip()
        if len(content) > 0:
            return content
        else:
            print("发布的内容为空，请输入发布内容\n")


def get_input_qos():
    qos = 0
    while True:
        try:
            qos = int(input("请输入发布 QoS 等级:0 or 1 or 2\n"))
            if (qos >= 0) and (qos < 3):
                return qos
            else:
                print("输入的 qos 不正确，请输入 0 or 1 or 2\n")
        except Exception as e:
            print("输入的 qos 不正确，请输入 0 or 1 or 2\n")


def get_publish_msg_length(data, rem_len):
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


def subscribe_topic():
    while True:
        select_oper = input("请输入sub or unsub or publish\n")
        if select_oper.lower().__eq__("sub"):
            topic = get_input_topic()
            qos = get_input_qos()
            package = package_subscribe_msg(topic, qos)
        elif select_oper.lower().__eq__("unsub"):
            topic = get_input_topic()
            package = package_unsubscribe_msg(topic)
        elif select_oper.lower().__eq__("publish"):
            topic = get_input_topic()
            content = get_input_payload()
            qos = get_input_qos()
            package = publish_topic(topic, content, qos)
        else:
            print("输入的选择项不存在\n")
            continue
        start_send(package)
        time.sleep(1)


def publish_topic(topic, content, qos):
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


def start_subscribe_topic_thread():
    watch_thread = threading.Thread(target=subscribe_topic)
    watch_thread.setDaemon(True)
    watch_thread.start()


def package_subscribe_msg(topic, qos=0):
    if subscribe_msg_dict.keys().__contains__(topic):
        return None
    else:
        global sub_msg_id
        value = get_bytearray(topic)
        sub_topic = TOPIC()
        sub_topic.set_topic(topic)
        sub_topic.set_qos(qos)
        result = bytearray()
        sub_topic.set_id(sub_msg_id)
        length = sub_msg_id & 0xFFFF
        sub_msg_id = sub_msg_id + 1
        hvalue = length >> 8
        lvalue = length & 0xFF
        result.append(hvalue)
        result.append(lvalue)
        result = result + value
        result.append(qos)

        data = bytearray()
        length = len(result)
        data.append(0x82)
        data.append(length)
        subscribe_msg = data + result
        subscribe_msg_dict[topic] = sub_topic
        return subscribe_msg


def package_unsubscribe_msg(topic):
    if subscribe_msg_dict.keys().__contains__(topic):
        global sub_msg_id
        value = get_bytearray(topic)
        result = bytearray()
        length = sub_msg_id & 0xFFFF
        sub_msg_id = sub_msg_id + 1
        hvalue = length >> 8
        lvalue = length & 0xFF
        result.append(hvalue)
        result.append(lvalue)
        result = result + value

        data = bytearray()
        length = len(result)
        data.append(0xa2)
        data.append(length)
        subscribe_msg = data + result
        del subscribe_msg_dict[topic]
        return subscribe_msg


def ping_req():
    while True:
        # print("***************")
        time.sleep(5)
        # print("ping req")
        data = package_ping_req_msg()
        start_send(data)


def keep_live():
    watch_thread = threading.Thread(target=ping_req)
    watch_thread.setDaemon(True)
    watch_thread.start()


def get_bytearray(value):
    result = bytearray()
    length = (len(value)) & 0xFFFF
    hvalue = length >> 8
    lvalue = length & 0xFF
    result.append(hvalue)
    result.append(lvalue)
    return result + bytearray(bytes(value, encoding="utf8"))


def package_connect_msg(client_id, username, password):
    mqtt_name = get_bytearray("MQTT")
    id = get_bytearray(client_id)
    name = get_bytearray(username)
    pw = get_bytearray(password)

    result = bytearray()
    result = result + mqtt_name
    result.append(0x04)  # MQTT版本
    result.append(0xc2)  # conn flags 需要用户名和密码认证
    result.append(0x00)  # Keep-alive Time Length MSB
    result.append(0x05)  # Keep-alive Time Length LSB
    result = result + id
    result = result + name
    result = result + pw

    data = bytearray()
    length = len(result)
    data.append(0x10)
    data.append(length)
    connect_msg = data + result
    return connect_msg


def package_ping_req_msg():
    data = bytearray()
    data.append(0xc0)
    data.append(0x00)
    return data


def package_disconnect():
    data = bytearray()
    data.append(0xe0)
    data.append(0x00)
    return data
'''
    102900044D51545404C2003C000A0102303330343035303600067075626C696300097061677075626C6963
    解析：
    // 固定报文头
    10 // 固定报文头 byte1
    29 // 固定报文头之byte2

    // 可变报文头
    0004 // 长度
    4D515454 // 内容(MQTT)
    04  // 协议级别
    C2 // 连接标志位
    003C // 保持连接时长（60秒）

    // 载体部分
    000A // 长度
    01023033303430353036 // 用户名

    0006
    7075626C6963 

    0009
    7061677075626C6963  // 密码
'''


def str_to_hex(s):
    return ''.join([hex(ord(c)).replace('0x', '') for c in s])


def hex_to_str(s):
    return ''.join([chr(i) for i in [int(b, 16) for b in s.split(' ')]])


def str_to_bin(s):
    return ' '.join([bin(ord(c)).replace('0b', '') for c in s])


def bin_to_str(s):
    return ''.join([chr(i) for i in [int(b, 2) for b in s.split(' ')]])


def start_connect_service(ip, port):
    global client
    connect_success = False
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((ip, port))
        connect_success = True
    except Exception as e:
        connect_success = False
        print('start connect error:', e)
    finally:
        return connect_success


def disconnect_service():
    global client
    start_send(package_disconnect())
    client.close()


def recv_data_from_service():
    print("开始接收数据")
    global client
    while True:
        try:
            data = client.recv(1024)
            length = len(data)
            # print("接收的数据长度为：%d ，具体数据为：%s"%(length, data))
            while length > 0:
                ctrl_msg = (int(data[0]) >> 4)
                # print("ctrl_msg=%d" % ctrl_msg)
                if ctrl_msg == const.Reserved:
                    return
                elif ctrl_msg == const.CONNACK:
                    msg_len = int(data[1])
                    data = data[msg_len+2:]
                    keep_live()
                    start_subscribe_topic_thread()
                elif ctrl_msg == const.PINGRESP:
                    # print("ping resp")
                    msg_len = int(data[1])
                    data = data[msg_len+2:]
                elif ctrl_msg == const.SUBACK:
                    msg_len = int(data[1])
                    data = data[msg_len+2:]
                    # print("suback")
                elif ctrl_msg == const.UNSUBACK:
                    msg_len = int(data[1])
                    data = data[msg_len+2:]
                    # print("unsuback")
                elif ctrl_msg == const.PUBLISH:
                    rem_len = mqtt_num_rem_len_bytes(data)
                    msg_len = get_publish_msg_length(data[1:rem_len+1], rem_len) + rem_len + 1
                    topic_length = int(data[rem_len+1])*256 + int(data[rem_len+2])
                    topic_name = data[rem_len+1+2:topic_length+rem_len+1+2].decode("utf-8")
                    recv_topic = subscribe_msg_dict.get(topic_name)
                    msg_id_len = 0
                    if recv_topic.get_qos() != 0:
                        msg_id_len = 2
                        msg_id = int(data[1+rem_len + 2 + topic_length])*256 + int(data[1+rem_len + 2 + topic_length + 1])
                        print("msg_id=%d" % msg_id)
                    if msg_len > length:
                        payload = data[1+rem_len + 2 + topic_length + msg_id_len:length].decode("utf-8")
                        print("msg_length=%d,topic_length=%d,topic_name=%s,payload=%s" %
                              (msg_len, topic_length, topic_name, payload))
                        wait_read_length = msg_len - length
                        read_length = 1024
                        while True:
                            remain_data = client.recv(read_length)
                            # print("wait_read_length=%d,remain_data_len=%d" % (wait_read_length, len(remain_data)))
                            if wait_read_length.__le__(read_length):
                                print(remain_data.decode("utf-8"))
                                data = []
                                break
                            wait_read_length = wait_read_length - len(remain_data)
                            if wait_read_length.__le__(read_length):
                                read_length = wait_read_length
                    else:
                        print(data[:])
                        payload = data[1+rem_len + 2 + topic_length + msg_id_len:msg_len].decode("utf-8")
                        print("msg_length=%d,topic_length=%d,topic_name=%s,payload=%s" %
                              (msg_len, topic_length, topic_name, payload))
                        data = data[msg_len:]
                elif ctrl_msg == const.PUBACK or ctrl_msg == const.PUBREC or ctrl_msg == const.PUBCOMP:
                    msg_len = int(data[1])
                    msg_id = int(data[2])*256 + int(data[3])
                    print("msg_id=%d" % msg_id)
                    if ctrl_msg == const.PUBREC:  # 5
                        package = bytearray()
                        package.append(0x62)
                        package.append(msg_len)
                        package.append(data[2])
                        package.append(data[3])
                        start_send(package)
                    elif ctrl_msg == const.PUBCOMP:  # 7
                        print("消息 msg_id=%d，发布完成" % msg_id)
                    data = data[msg_len + 2:]
                else:
                    print("ctrl_msg=%d" % ctrl_msg)
                length = len(data)
                # print("length=%d" % length)
        except Exception as e:
            print('recv_data_from_service Error:', e)
            break


def start_connect_mqtt_server(client_id, username, password):
    data = package_connect_msg(client_id, username, password)
    result = start_send(data)
    if result:
        data_thread = threading.Thread(target=recv_data_from_service)
        data_thread.setDaemon(True)
        data_thread.start()
        data_thread.join()


def start_join_service(ip, port, client_id, username, password):
    global global_client_id
    global global_username
    global global_password
    global global_ip
    global global_port

    global_client_id = client_id
    global_username = username
    global_password = password
    global_ip = ip
    global_port = port

    try:
        connect_success = start_connect_service(ip, port)
        if connect_success:
            start_connect_mqtt_server(client_id, username, password)
        else:
            print("connect service error")
    except Exception as e:
        print('start_join_service Error:', e)
    finally:
        pass


def start_send(data):
    global client
    result = False
    try:
        client.send(data)
        result = True
    except Exception as e:
        result = False
        print(e)
    finally:
        return result

# get_connect_data("claa_01", "admin", "public")


start_join_service("100.0.0.12", 1883, "claa_01", "admin", "public")

