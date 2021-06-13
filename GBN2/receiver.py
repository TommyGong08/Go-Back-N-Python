import pickle
import socket
import random
import threading
from Utils import Config, RecvLogConfig, CRC

recv_config = Config()  #读取配置文件
recv_log_config = RecvLogConfig()  #读取日志配置
recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  #服务端socket
recv_socket.bind(recv_config.recv_addr)
recv_file = open(recv_config.recv_file, 'wb')  #以二进制写方式打开文件，没有则创建文件


#是否丢失
def should_lost():
    global recv_config
    #产生一个1~lost_rate的随机数，如果刚好等于lost_rate返回True
    return True if random.randint(1, recv_config.lost_rate) == recv_config.lost_rate else False


#接受数据帧
def receive_pdu():
    global recv_config
    global recv_log_config
    global recv_socket
    global recv_file
    recv_log = open(recv_config.recv_log, 'w')  #创建日志
    while True:
        pdu, send_addr = recv_socket.recvfrom(recv_config.data_size * 2)  #接受数据帧
        if should_lost():  #数据帧丢失
            continue
        else:  #未丢失
            recv_log_config.num_to_recv += 1  #接受次数+1
            pdu = pickle.loads(pdu)  #解析数据
            recv_log_config.pdu_recv = pdu['pdu_to_send']  #获取接受的pdu序号
            if CRC().calculate(pdu['data']) != int(pdu['checksum']):  #数据出现错误
                recv_log_config.status = 'DataErr'
                log = recv_log_config.get_log()  #获取日志
                print(log)
            else:  #数据未出错
                if recv_log_config.pdu_exp != pdu['pdu_to_send']:  #序号错误
                    recv_log_config.status = 'NoErr'
                    log = recv_log_config.get_log()  #获取日志
                    print(log)
                else:  #正确接收
                    recv_log_config.status = 'OK'
                    log = recv_log_config.get_log()  #获取日志
                    print(log)
                    ack = recv_log_config.pdu_exp  #获取应该返回的ack
                    threading.Thread(target=send_ack, args=(pickle.dumps(ack), send_addr)).start()  #返回ack
                    if len(pdu['data']) == 0:  #收到空数据，文件已发送完毕
                        recv_file.close()
                        print('receive complete')
                        recv_log.write('receive complete\n')
                        recv_log.close()
                        return
                    recv_file.write(pdu['data'])  #将收到的数据写入文件
                    recv_log_config.pdu_exp += 1
            recv_log.write(log + '\n')  #写入日志


#返回ack
def send_ack(ack, send_addr):
    global recv_socket
    recv_socket.sendto(ack, send_addr)


if __name__ == '__main__':
    receive_pdu()
