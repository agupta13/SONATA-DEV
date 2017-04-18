from scapy.all import IP, TCP, UDP, Ether
import struct
from multiprocessing.connection import Listener
import time
import logging


class Emitter(object):
    def __init__(self, conf, id_to_qid_mapping, out_headers):
        # Interfaces
        print "********* EMITTER INITIALIZED *********"
        self.spark_stream_address = conf['spark_stream_address']
        self.spark_stream_port = conf['spark_stream_port']
        self.sniff_interface = conf['sniff_interface']

        self.listener = Listener((self.spark_stream_address, self.spark_stream_port))
        self.spark_conn = None

        # dict mapping table id to query id
        self.id_to_qid = id_to_qid_mapping
        # dict mapping query id to the expected headers in the tuple in order
        self.out_headers = out_headers

        # create a logger for the object
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        # create file handler which logs messages
        self.fh = logging.FileHandler(conf['log_file'])
        self.fh.setLevel(logging.INFO)
        self.logger.addHandler(self.fh)

    def start(self):
        while True:
            print "Waiting for socket"
            self.spark_conn = self.listener.accept()
            print "Now start sniffing the packets from switch"
            self.sniff_packets()

    def send_data(self, data):
        self.spark_conn.send_bytes(data)

    def sniff_packets(self):
        sniff(iface=self.sniff_interface, prn=lambda x: self.process_packet(x))

    def process_packet(self, packet):
        '''
        callback function executed for each capture packet
        '''

        # Extract all the headers according to out_headers
        if Ether in packet:
            src_mac = packet[Ether].src

            query_id = self.id_to_qid[int(src_mac)]
            out_headers = self.out_headers[query_id]

            packet_tuple = list()
            for out_header in out_headers:
                if 'sIP' == out_header and IP in packet:
                    packet_tuple.append(packet[IP].src)
                elif 'dIP' == out_header and IP in packet:
                    packet_tuple.append(packet[IP].dst)
                elif 'sPort' == out_header and IP in packet and UDP in packet:
                    packet_tuple.append(packet[UDP].src)
                elif 'dPort' == out_header and IP in packet and UDP in packet:
                    packet_tuple.append(packet[UDP].dst)
                elif 'sPort' == out_header and IP in packet and TCP in packet:
                    packet_tuple.append(packet[TCP].src)
                elif 'dPort' == out_header and IP in packet and TCP in packet:
                    packet_tuple.append(packet[TCP].dst)
                elif 'sMac' == out_header and Ether in packet:
                    packet_tuple.append(packet[Ether].src)
                elif 'dMac' == out_header and Ether in packet:
                    packet_tuple.append(packet[Ether].dst)
                elif 'proto' == out_header and IP in packet:
                    packet_tuple.append(packet[IP].proto)

            self.send_data(",".join([str(x) for x in packet_tuple]) + "\n")

