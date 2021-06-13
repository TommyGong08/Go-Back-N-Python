'''
在host1上修改，合并两个接收端，旨在区分ack和数据报
host2作为服务端
'''
import threading
import pickle
import socket
from Utils2 import PDU, Config, CRC, RecvLogConfig
import os
import signal
import random
import time

host2_config = Config()  # 读取配置文件
event = threading.Event()  # event flag
lock = threading.RLock()  # 递归锁
host2_send_file = open(host2_config.send_file, 'rb')  # 以二进制方式打开待发送的文件，测试发送一份pdf文件
host2_send_log = open(host2_config.send_log, 'w')  # 创建日志
host2_recv_log_config = RecvLogConfig()  # 读取日志配置
host2_recv_file = open(host2_config.recv_file, 'wb')  # 以二进制写方式打开文件，没有则创建文件

'''
# 是否产生错误
def should_error():
    global host2_config
    # 产生一个1~error_rate的随机数，如果刚好等于error_rate返回True
    return True if random.randint(1, host2_config.error_rate) == host2_config.error_rate else False
'''

# 填充pdu队列
def fill_pdu_q():
    global host2_config
    global host2_send_file
    while True:
        # 维护滑动窗口，当已发送的pdu与已收到的ack之差小于等于窗口大小时才生成新的pdu
        while host2_config.pdu_to_send - host2_config.acked_num <= host2_config.sw_size:
            if host2_config.pdu_to_send > host2_config.pdu_sum + 1:  # 发送完文件后再发送一份空数据
                break
            lock.acquire()  # 加锁
            host2_send_file.seek((host2_config.pdu_to_send - 1) * host2_config.data_size)  # 使文件指针指向当前应该读取的字段
            data = host2_send_file.read(host2_config.data_size)  # 读取指定大小的内容
            checksum = CRC().calculate(data)    # 计算checksum
            '''
            if should_error():  # 模拟出错
                checksum += 1
            '''
            pdu = PDU(is_ack=-1,
                      num_to_send=host2_config.num_to_send,
                      pdu_to_send=host2_config.pdu_to_send,
                      status=('NEW' if host2_config.pdu_to_send > host2_config.pdu_to_resend else 'RT'),
                      acked_num=host2_config.acked_num,
                      data=data,
                      checksum='%d' % checksum)
            host2_config.pdu_q.put(pdu)  # 加入队列
            host2_config.num_to_send += 1  # 发送次数+1
            host2_config.pdu_to_send += 1  # 发送pdu数+1
            lock.release()  # 释放锁
        event.set()  # 设置event为ture，唤醒send_frame线程


# 发送pdu队列
def send_pdu():
    global host2_config
    global host2_socket
    global host2_send_log
    print('%d pdus to be send' % host2_config.pdu_sum)
    event.wait()  # 阻塞等待fill_pdu_q线程
    while True:
        lock.acquire()  # 加锁
        while not host2_config.pdu_q.empty():  # pdu队列不空时发送
            pdu = host2_config.pdu_q.get()
            host2_socket.sendto(pickle.dumps(pdu.get_pdu()), host2_config.host1_addr)  # frame发送至服务端
            log = pdu.get_log()  # 获取日志
            host2_send_log.write(log + '\n')  # 写入日志
            print(log)
        lock.release()  # 释放锁
        event.clear()  # 设置event为false
        event.wait()  # 阻塞等待fill_pud_q队列


# 开启重发
def resend():
    global host2_config
    lock.acquire()
    host2_config.pdu_to_resend = host2_config.pdu_to_send - 1  # 设置需要重发的pdu序号
    host2_config.pdu_to_send = host2_config.acked_num + 1  # 重新设置已经发送的pdu序号
    print('resend pdu from %d to %d' % (host2_config.pdu_to_send, host2_config.pdu_to_resend))
    lock.release()


# 是否丢失
def should_lost():
    global host2_config
    # 产生一个1~lost_rate的随机数，如果刚好等于lost_rate返回True
    return True if random.randint(1, host2_config.lost_rate) == host2_config.lost_rate else False


# 接受端线程
def receiver():
    global host2_config
    global host2_socket
    global host2_send_file
    global host2_send_log

    global host2_recv_log_config
    global host2_recv_file
    last_time = time.time()
    recv_log = open(host2_config.recv_log, 'w')  # 创建日志
    while True:
        if time.time() - last_time > host2_config.timeout:
            if host2_config.acked_num - 1 == host2_config.pdu_sum:
                pass
            else:
                threading.Thread(name='resend_pdu', target=resend).start()
                last_time = time.time()
        # host2_socket.settimeout(host2_config.timeout)  # 设置阻塞接受时间
        try:
            pdu, host1_addr = host2_socket.recvfrom(host2_config.data_size * 2)  # 接受数据帧
            pdu = pickle.loads(pdu)  # pickle可以读取dict和list的数据
        except ConnectionResetError:
            continue
        except BlockingIOError:
            continue

        if pdu['is_ack'] == -1:  # 如果是数据帧
            # if should_lost():  # 数据帧丢失
                    # continue
            # 如果未丢失，判断是数据帧还是ack
            host2_recv_log_config.num_to_recv += 1  # 接受次数+1
            # pdu = pickle.loads(pdu)  # 解析数据
            host2_recv_log_config.pdu_recv = pdu['pdu_to_send']  # 获取接受的pdu序号
            if CRC().calculate(pdu['data']) != int(pdu['checksum']):  # 数据出现错误
                host2_recv_log_config.status = 'DataErr'
                log = host2_recv_log_config.get_log()  # 获取日志
                print(log)
            else:  # 数据未出错
                if host2_recv_log_config.pdu_exp != pdu['pdu_to_send']:  # 序号错误
                    host2_recv_log_config.status = 'NoErr'
                    log = host2_recv_log_config.get_log()  # 获取日志
                    print(log)
                else:  # 正确接收
                    host2_recv_log_config.status = 'OK'
                    log = host2_recv_log_config.get_log()  # 获取日志
                    print(log)
                    # ack = host2_recv_log_config.pdu_exp  # 获取应该返回的ack
                    ack = PDU(is_ack=-2,
                                num_to_send=-1,
                                pdu_to_send=-1,
                                status="OK",
                                acked_num=-1,
                                data=host2_recv_log_config.pdu_exp,
                                checksum=-1)
                    threading.Thread(target=send_ack,
                                        args=(pickle.dumps(ack.get_pdu()), host1_addr)).start()   # 返回ack
                    if len(pdu['data']) == 0:  # 收到空数据，文件已发送完毕
                        host2_send_file.close()
                        print('receive complete')
                        recv_log.write('receive complete\n')
                        recv_log.close()
                        host2_recv_file.close()
                        return
                    host2_recv_file.write(pdu['data'])  # 将收到的数据写入文件
                    host2_recv_log_config.pdu_exp += 1
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
            host2_config.acked_num = ack  # 修改已收到的ack
            print('receive ack: ', ack)
            if host2_config.acked_num == host2_config.pdu_sum + 1:  # 所有ack接受完毕，结束进程
                print('send complete')
                host2_send_log.write('send complete\n')
                host2_send_file.close()
                host2_send_log.close()
                os.kill(os.getpid(), signal.SIGTERM)
            lock.release()


# 返回ack
def send_ack(ack, recv_addr):
    global host2_socket
    host2_socket.sendto(ack, recv_addr)


if __name__ == '__main__':

    host2_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 客户端socket

    # 阻塞模式
    host2_socket.setblocking(1)
    host2_socket.settimeout(10)
    host2_socket.bind(host2_config.host2_addr)  # 服务器绑定发送端的端口
    print("绑定成功")

    receiver_thread = threading.Thread(name='receiver', target=receiver)
    receiver_thread.start()

    print("0: 解除绑定 1: 传送文件")
    cmd = input("输入指令:")
    print(cmd)
    if cmd == "0":
        # 解除socket绑定
        host2_socket.close()

    elif cmd == "1":
        fill_pdu_q = threading.Thread(name='fill_pud_q', target=fill_pdu_q)
        send_pdu = threading.Thread(name='send_pdu', target=send_pdu)
        # receiver_ack = threading.Thread(name='receive_ack', target=receive_ack)
        # receiver_thread = threading.Thread(name='receiver', target=receiver)

        fill_pdu_q.start()
        send_pdu.start()
        # receiver_ack.start()

