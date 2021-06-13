import socket
import os
import math
from queue import Queue
from ctypes import c_ushort


# 发送的数据帧
class PDU:
    def __init__(self, is_ack, num_to_send=-1, pdu_to_send=-1, status='OK', acked_num=-1, data=-1, checksum=-1):
        self.is_ack = is_ack            # 区分该帧是数据帧还是ack帧, -1表示数据，-2表示ack
        self.num_to_send = num_to_send  # 顺序编号，第几次发送
        self.pdu_to_send = pdu_to_send  # 本次发送的pdu的序号
        self.status = status            # 发送状态，新New，超时重传RT
        self.acked_num = acked_num      # 当前已被确认的 PDU 的序号
        self.data = data                # 待发送的数据本体
        self.checksum = checksum        # 校验字段

    def get_pdu(self):
        # 使用字典封装frame
        pdu = {'is_ack': self.is_ack,
                 'num_to_send': self.num_to_send,
                 'pdu_to_send': self.pdu_to_send,
                 'status': self.status,
                 'acked_num': self.acked_num,
                 'data': self.data,
                 'checksum': self.checksum}
        return pdu

    # 获取当前日志
    def get_log(self):
        log = "%d, pdu_to_send=%d, status=%s, ackedNo=%d" % (self.num_to_send, self.pdu_to_send, self.status, self.acked_num)
        return log


# 一些全局需要使用变量，封装到这个配置文件里
class Config:
    def __init__(self):
        self.ip = socket.gethostbyname(socket.gethostname())    # ip地址
        self.host1_port = 41123                                 # host1的端口
        self.host1_addr = (self.ip, self.host1_port)            # host1的地址
        self.host2_port = 41897                                 # host2的端口
        self.host2_addr = (self.ip, self.host2_port)            # host2的地址

        self.data_size = 4096   # 数据长度
        self.error_rate = 100   # 错误率
        self.lost_rate = 100    # 丢失率
        self.sw_size = 8        # 发送窗口大小
        self.init_seq_num = 1   # 起始序号
        self.timeout = 1        # 超时时间

        self.pdu_to_send = self.init_seq_num        # 当前发送的pdu序号
        self.num_to_send = self.init_seq_num        # 当前发送的次数
        self.acked_num = 0                          # 当前已被确认的 PDU 的序号
        self.pdu_to_resend = 0                      # 需要重发的pdu序号，小于等于该序号需重发
        self.pdu_q = Queue(maxsize=self.sw_size)    # 待发送的pdu队列

        self.send_file = 'test1.pdf'  # 发送的文件
        self.send_log = self.send_file + '.host2.log'  # 发送方日志
        self.recv_file = 'host2_recv.pdf'  # 接受后写入的文件
        self.recv_log = self.recv_file + '.log'  # 接收方日志
        self.pdu_sum = math.ceil(os.path.getsize(self.send_file) / self.data_size)  # 需要发送的pdu总数


# 服务端（接收方）的日志信息
class RecvLogConfig:
    def __init__(self):
        self.num_to_recv = 0    # 当前接受的次数
        self.pdu_exp = 1        # 期望收到的pdu
        self.pdu_recv = 0       # 当前收到的pdu
        self.status = ''        # 当前收到的pdu的状态，数据错误DataErr，序号错误NoErr，正确OK

    # 获取当前日志
    def get_log(self):
        log = "%d, pdu_exp=%d, pdu_recv=%d, status=%s" % (self.num_to_recv, self.pdu_exp, self.pdu_recv, self.status)
        return log


# 计算CRC CCITT
class CRC():
    crc_ccitt_tab = []
    # The CRC's are computed using polynomials.
    # Here is the most used coefficient for CRC CCITT
    crc_ccitt_constant = 0x1021

    def __init__(self, version='XModem'):
        try:
            dict_versions = {'XModem': 0x0000, 'FFFF': 0xffff, '1D0F': 0x1d0f}
            if version not in dict_versions.keys():
                raise Exception("Your version parameter should be one of \
                    the {} options".format("|".join(dict_versions.keys())))

            self.starting_value = dict_versions[version]

            # initialize the precalculated tables
            if not len(self.crc_ccitt_tab):
                self.init_crc_ccitt()
        except Exception as e:
            print("EXCEPTION(calculate): {}".format(e))

    def calculate(self, input_data=None):
        try:
            is_string = isinstance(input_data, str)
            is_bytes = isinstance(input_data, bytes)

            if not is_string and not is_bytes:
                raise Exception("Please provide a string or a byte sequence as argument for calculation.")

            crcValue = self.starting_value

            for c in input_data:
                d = ord(c) if is_string else c
                tmp = (c_ushort(crcValue >> 8).value) ^ d
                crcValue = (c_ushort(crcValue << 8).value) ^ int(
                    self.crc_ccitt_tab[tmp], 0)

            return crcValue
        except Exception as e:
            print("EXCEPTION(calculate): {}".format(e))

    def init_crc_ccitt(self):
        '''The algorithm uses tables with precalculated values'''
        for i in range(0, 256):
            crc = 0
            c = i << 8

            for j in range(0, 8):
                if (crc ^ c) & 0x8000:
                    crc = c_ushort(crc << 1).value ^ self.crc_ccitt_constant
                else:
                    crc = c_ushort(crc << 1).value

                c = c_ushort(c << 1).value  # equivalent of c = c << 1
            self.crc_ccitt_tab.append(hex(crc))