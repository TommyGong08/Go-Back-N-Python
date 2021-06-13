'''
在host1上修改，合并两个接收端，旨在区分ack和数据报
host1作为客户端
'''

import threading
import pickle
import socket
from Utils3 import PDU, Config, CRC, RecvLogConfig
import os
import signal
import random
import time

host1_config = Config()  # 读取配置文件
event = threading.Event()  # event flag
lock = threading.RLock()  # 递归锁
host1_send_file = open(host1_config.send_file, 'rb')  # 以二进制方式打开待发送的文件，测试发送一份pdf文件
host1_send_log = open(host1_config.send_log, 'w')  # 创建日志
host1_recv_log_config = RecvLogConfig()  # 读取日志配置
host1_recv_file = open(host1_config.recv_file, 'wb')  # 以二进制写方式打开文件，没有则创建文件

'''
# 是否产生错误
def should_error():
    global host1_config
    # 产生一个1~error_rate的随机数，如果刚好等于error_rate返回True
    return True if random.randint(1, host1_config.error_rate) == host1_config.error_rate else False
'''

# 填充pdu队列
def fill_pdu_q():
    global time
    global host1_config
    global host1_send_file
    while True:
        # 维护滑动窗口，当已发送的pdu与已收到的ack之差小于等于窗口大小时才生成新的pdu
        while host1_config.pdu_to_send - host1_config.acked_num <= host1_config.sw_size:
            if host1_config.pdu_to_send > host1_config.pdu_sum + 1:  # 发送完文件后再发送一份空数据
                break
            lock.acquire()  # 加锁
            host1_send_file.seek((host1_config.pdu_to_send - 1) * host1_config.data_size)  # 使文件指针指向当前应该读取的字段
            data = host1_send_file.read(host1_config.data_size)  # 读取指定大小的内容
            checksum = CRC().calculate(data)    # 计算checksum
            # 模拟不出错
            '''
            if should_error():  # 模拟出错
                checksum += 1
            '''
            pdu = PDU(is_ack=-1,
                      num_to_send=host1_config.num_to_send,
                      pdu_to_send=host1_config.pdu_to_send,
                      status=('NEW' if host1_config.pdu_to_send > host1_config.pdu_to_resend else 'RT'),
                      acked_num=host1_config.acked_num,
                      data=data,
                      checksum='%d'%checksum)
            host1_config.pdu_q.put(pdu)  # 加入队列
            host1_config.num_to_send += 1  # 发送次数+1
            host1_config.pdu_to_send += 1  # 发送pdu数+1
            lock.release()  # 释放锁
        event.set()  # 设置event为ture，唤醒send_frame线程


# 发送pdu队列
def send_pdu():
    global host1_config
    global host1_socket
    global host1_send_log
    print('%d pdus to be send' % host1_config.pdu_sum)
    event.wait()  # 阻塞等待fill_pdu_q线程
    while True:
        lock.acquire()  # 加锁
        while not host1_config.pdu_q.empty():  # pdu队列不空时发送
            pdu = host1_config.pdu_q.get()
            host1_socket.sendto(pickle.dumps(pdu.get_pdu()), host1_config.host2_addr)  # frame发送给host2
            log = pdu.get_log()  # 获取日志
            host1_send_log.write(log + '\n')  # 写入日志
            print(log)
        lock.release()  # 释放锁
        event.clear()  # 设置event为false
        event.wait()  # 阻塞等待fill_pud_q队列


# 开启重发
def resend():
    global host1_config
    lock.acquire()
    host1_config.pdu_to_resend = host1_config.pdu_to_send - 1  # 设置需要重发的pdu序号
    host1_config.pdu_to_send = host1_config.acked_num + 1  # 重新设置已经发送的pdu序号
    print('resend pdu from %d to %d' % (host1_config.pdu_to_send, host1_config.pdu_to_resend))
    lock.release()


# 是否丢失
def should_lost():
    global host1_config
    # 产生一个1~lost_rate的随机数，如果刚好等于lost_rate返回True
    return True if random.randint(1, host1_config.lost_rate) == host1_config.lost_rate else False


# 接受端线程
def receiver():
    print("In the receiver process!")
    global host1_config
    global host1_socket
    global host1_send_file
    global host1_send_log

    global host1_recv_log_config
    global host1_recv_file
    last_time = time.time()
    recv_log = open(host1_config.recv_log, 'w')  # 创建日志
    while True:
        if time.time() - last_time > host1_config.timeout:
            if host1_config.acked_num-1 == host1_config.pdu_sum:
                pass
            else:
                threading.Thread(name='resend_pdu', target=resend).start()
                last_time = time.time()
        # host1_socket.settimeout(host1_config.timeout)  # 设置阻塞接受时间
        try:
            pdu, host2_addr = host1_socket.recvfrom(host1_config.data_size * 2)  # 接受数据帧
            pdu = pickle.loads(pdu)
            # pdu数据类型bytes
        except ConnectionResetError:
            continue
        except BlockingIOError:
            continue
        # print(type(pdu))
        if pdu['is_ack'] == -1:  # 如果是数据帧
            host1_recv_log_config.num_to_recv += 1  # 接受次数+1
            # pdu = pickle.loads(pdu)  # 解析数据
            host1_recv_log_config.pdu_recv = pdu['pdu_to_send']  # 获取接受的pdu序号
            if CRC().calculate(pdu['data']) != int(pdu['checksum']):  # 数据出现错误
                host1_recv_log_config.status = 'DataErr'
                log = host1_recv_log_config.get_log()  # 获取日志
                print(log)
            else:  # 数据未出错
                if host1_recv_log_config.pdu_exp != pdu['pdu_to_send']:  # 序号错误
                    host1_recv_log_config.status = 'NoErr'
                    log = host1_recv_log_config.get_log()  # 获取日志
                    print(log)
                else:  # 正确接收
                    host1_recv_log_config.status = 'OK'
                    log = host1_recv_log_config.get_log()  # 获取日志
                    print(log)
                    # ack = host2_recv_log_config.pdu_exp  # 获取应该返回的ack
                    ack = PDU(is_ack=-2,
                                num_to_send=-1,
                                pdu_to_send=-1,
                                status="OK",
                                acked_num=-1,
                                data=host1_recv_log_config.pdu_exp,
                                checksum=-1)
                    threading.Thread(target=send_ack,
                                        args=(pickle.dumps(ack.get_pdu()), host2_addr)).start()  # 返回ack
                    if len(pdu['data']) == 0:  # 收到空数据，文件已发送完毕
                        host1_send_file.close()
                        print('receive complete')
                        recv_log.write('receive complete\n')
                        recv_log.close()
                        host1_recv_file.close()
                        return
                    host1_recv_file.write(pdu['data'])  # 将收到的数据写入文件
                    host1_recv_log_config.pdu_exp += 1
            recv_log.write(log + '\n')  # 写入日志
        elif pdu['is_ack'] == -2:  # 是ack帧则接受
            last_time = time.time()
            try:
                # ack = host1_socket.recvfrom(1024)[0]
                ack = pdu['data']
            except socket.timeout:  # 超时重发
                print("resend")
                threading.Thread(name='resend', target=resend).start()
                continue

            # ack = pickle.loads(ack)
            lock.acquire()
            host1_config.acked_num = ack  # 修改已收到的ack
            print('receive ack: ', ack)
            if host1_config.acked_num == host1_config.pdu_sum + 1:  # 所有ack接受完毕，结束进程
                print('send complete')
                host1_send_log.write('send complete\n')
                host1_send_file.close()
                host1_send_log.close()
                os.kill(os.getpid(), signal.SIGTERM)
            lock.release()
        print("receiver ended!")


# 返回ack
def send_ack(ack, recv_addr):
    global host1_socket
    host1_socket.sendto(ack, recv_addr)


if __name__ == '__main__':

    host1_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 客户端socket
    receiver_thread = threading.Thread(name='receiver', target=receiver)
    receiver_thread.start()
    # host1_socket.bind(host1_config.host1_addr)  # 绑定发送端口
    # 阻塞模式
    host1_socket.setblocking(1)
    host1_socket.settimeout(5)


    print("0: 解除绑定 1: 传送文件")
    cmd = input("输入指令:")
    if cmd == "0":
        print("!!!!!")
        # 解除socket绑定
        host1_socket.close()

    elif cmd == "1":
        print("#####")

        fill_pdu_q = threading.Thread(name='fill_pud_q', target=fill_pdu_q)
        send_pdu = threading.Thread(name='send_pdu', target=send_pdu)
        # receiver_ack = threading.Thread(name='receive_ack', target=receive_ack)
        # receiver_thread = threading.Thread(name='receiver', target=receiver)

        fill_pdu_q.start()
        send_pdu.start()
