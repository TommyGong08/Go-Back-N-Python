import threading
import pickle
import socket
from Utils import PDU, Config, CRC
import os
import signal
import random

send_config = Config()  #读取配置文件
event = threading.Event()  #event flag
lock = threading.RLock()  #递归锁
send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  #客户端socket
send_socket.bind(send_config.send_addr)
send_file = open(send_config.send_file, 'rb')  #以二进制方式打开待发送的文件，测试发送一份pdf文件
send_log = open(send_config.send_log, 'w')  #创建日志


# 是否产生错误
def should_error():
    global host2_config
    # 产生一个1~error_rate的随机数，如果刚好等于error_rate返回True
    return True if random.randint(1, send_config.error_rate) == send_config.error_rate else False


# 填充pdu队列
def fill_pdu_q():
    global send_config
    global send_file
    while True:
        # 维护滑动窗口，当已发送的pdu与已收到的ack之差小于等于窗口大小时才生成新的pdu
        while send_config.pdu_to_send - send_config.acked_num <= send_config.sw_size:
            if send_config.pdu_to_send > send_config.pdu_sum + 1:  #发送完文件后再发送一份空数据
                break
            lock.acquire()  #加锁
            send_file.seek((send_config.pdu_to_send - 1) * send_config.data_size)  #使文件指针指向当前应该读取的字段
            data = send_file.read(send_config.data_size)  #读取指定大小的内容
            checksum = CRC().calculate(data)    #计算checksum
            if should_error():  #模拟出错
                checksum += 1
            pdu = PDU(num_to_send=send_config.num_to_send,
                      pdu_to_send=send_config.pdu_to_send,
                      status=('NEW' if send_config.pdu_to_send > send_config.pdu_to_resend else 'RT'),
                      acked_num=send_config.acked_num,
                      data=data,
                      checksum='%d'%checksum)
            send_config.pdu_q.put(pdu)  #加入队列
            send_config.num_to_send += 1  #发送次数+1
            send_config.pdu_to_send += 1  #发送pdu数+1
            lock.release()  #释放锁
        event.set()  #设置event为ture，唤醒send_frame线程


#发送pdu队列
def send_pdu():
    global send_config
    global send_socket
    global send_log
    print('%d pdus to be send' % send_config.pdu_sum)
    event.wait()  #阻塞等待fill_pdu_q线程
    while True:
        lock.acquire()  #加锁
        while not send_config.pdu_q.empty():  #pdu队列不空时发送
            pdu = send_config.pdu_q.get()
            send_socket.sendto(pickle.dumps(pdu.get_pdu()), send_config.recv_addr)  #frame发送至服务端
            log = pdu.get_log()  #获取日志
            send_log.write(log + '\n')  #写入日志
            print(log)
        lock.release()  #释放锁
        event.clear()  #设置event为false
        event.wait()  #阻塞等待fill_pud_q队列


#接收ack
def receive_ack():
    global host2_config
    global host2_socket
    global host2_send_file
    global host2_send_log
    while True:
        # print("等待接收%d" % (config.acked_num+1))
        send_socket.settimeout(send_config.timeout)  # 设置阻塞接受时间
        try:
            ack = send_socket.recvfrom(1024)[0]
        except socket.timeout:  # 超时重发
            threading.Thread(name='resend', target=resend).start()
            continue
        ack = pickle.loads(ack)
        lock.acquire()
        send_config.acked_num = ack  # 修改已收到的ack
        print('receive ack: ', ack)
        if send_config.acked_num == send_config.pdu_sum + 1:  #所有ack接受完毕，结束进程
            print('send complete')
            send_log.write('send complete\n')
            send_file.close()
            send_log.close()
            os.kill(os.getpid(), signal.SIGTERM)
        lock.release()


#开启重发
def resend():
    global host2_config
    lock.acquire()
    send_config.pdu_to_resend = send_config.pdu_to_send - 1  #设置需要重发的pdu序号
    send_config.pdu_to_send = send_config.acked_num + 1  #重新设置已经发送的pdu序号
    print('resend pdu from %d to %d' % (send_config.pdu_to_send, send_config.pdu_to_resend))
    lock.release()


if __name__ == '__main__':
    fill_pdu_q = threading.Thread(name='fill_pud_q', target=fill_pdu_q)
    send_pdu = threading.Thread(name='send_pdu', target=send_pdu)
    receiver_ack = threading.Thread(name='receive_ack', target=receive_ack)
    fill_pdu_q.start()
    send_pdu.start()
    receiver_ack.start()
