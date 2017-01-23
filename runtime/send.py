from scapy.all import *
import os
import sys
import glob
import math, time
import pickle
from multiprocessing.connection import Listener

def load_data():
    print "load_data called"
    data = {}
    fname = "/home/arp/SONATA-DEV/data/udp_traffic_equinix-chicago.20160121-130000.UTC.csv"
    with open(fname, 'r') as f:
        for line in f:
            tmp = line.split("\n")[0].split(",")
            ts = round(float(line.split(",")[0]),0)
            if ts not in data:
                data[ts] = []
            data[ts].append(tuple(line.split("\n")[0].split(",")))
    print "Number of TS: ", len(data.keys())
    return data


def send_packet(pkt_tuple):
    (sIP, sPort, dIP, dPort, nBytes, proto, sMac, dMac) = pkt_tuple
    p = Ether() / IP(dst=dIP, src=sIP) / TCP(dport=int(dPort), sport=int(sPort)) / "SONATA"
    sendp(p, iface = "out-veth-1", verbose=0)


def create_attack_traffic():
    number_of_packets = 100
    dIP = '121.7.186.25'
    sIPs = []
    attack_packets = []

    for i in range(number_of_packets):
        sIPs.append(socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff))))

    for sIP in sIPs:
        p = Ether() / IP(dst=dIP, src=sIP) / TCP() / "ATTACK"
        attack_packets.append(p)

    return attack_packets

def compose_packet(pkt_tuple):
    (sIP, sPort, dIP, dPort, nBytes, proto, sMac, dMac) = pkt_tuple
    p = Ether() / IP(dst=dIP, src=sIP) / TCP(dport=int(dPort), sport=int(sPort)) / "SONATA"
    return p


def send_dummy_packets(mode):
    if mode == 0:
        sIPs = ['112.7.186.20', '112.7.186.19', '112.7.186.19', '112.7.186.18']
        for sIP in sIPs:
            p = Ether() / IP(dst='112.7.186.25', src=sIP) / TCP() / "SONATA"
            p.summary()
            sendp(p, iface = "out-veth-1", verbose=0)
    else:
        sIPs = ['121.7.186.20', '121.7.186.19', '121.7.186.19', '121.7.186.18']
        for sIP in sIPs:
            p = Ether() / IP(dst='121.7.186.25', src=sIP) / TCP() / "ATTACK"
            p.summary()
            sendp(p, iface = "out-veth-1", verbose=0)



def send_dummy_packets_stream():
    sIPs = ['112.7.186.20', '112.7.186.19', '112.7.186.19', '112.7.186.18']
    for sIP in sIPs:
        send_tuple = ",".join([qid, dIP, sIP])+"\n"
        print "Tuple: ", send_tuple

class PicklablePacket:
    """A container for scapy packets that can be pickled (in contrast
    to scapy packets themselves)."""
    def __init__(self, pkt):
        self.contents = str(pkt)
        self.time = pkt.time

    def __call__(self):
        """Get the original scapy packet."""
        pkt = scapy.Ether(self.contents)
        pkt.time = self.time
        return pkt

def send_packets(use_composed = True):
    '''
    reads packets from IPFIX data file,
    sorts the time stamps, and
    sends them at regular intervals to P4-enabled switch.
    '''
    ipfix_data = load_data()
    ordered_ts = ipfix_data.keys()
    ordered_ts.sort()
    print "Total flow entries:", len(ordered_ts)

    composed_packets = {}

    if use_composed:
        with open("/home/arp/SONATA-DEV/data/composed_packets.pickle",'r') as f:
            composed_packets = pickle.load(f)
            #print composed_packets

    else:
        start = time.time()
        for ts in ordered_ts:
            composed_packets[ts] = []
            pkt_tuples = ipfix_data[ts][:1000]
            for pkt_tuple in pkt_tuples:
                composed_packets[ts].append(compose_packet(pkt_tuple[2:]))

        with open("composed_packets.pickle",'w') as f:
            pickle.dump(composed_packets, f)

        end = time.time()
        print "Time to compose: ", str(end-start)

    ctr = 1
    for ts in ordered_ts:
        start = time.time()
        print "Number of packets in:", ts, " are ", len(composed_packets[ts])
        #outgoing_packets = composed_packets[ts]
        outgoing_packets = composed_packets[ts]
        if ctr >= 10 and ctr <=15:
            print "Sending Attack traffic..."
            attack_packets = create_attack_traffic()
            sendp(attack_packets, iface = "out-veth-1", verbose=0)
            #outgoing_packets.extend(attack_packets)

        #for packet in outgoing_packets:
        sendp(outgoing_packets, iface = "out-veth-1", verbose=0)
        total = time.time()-start
        sleep_time = 1-total
        print "Finished Sending...", str(total), "sleeping for: ", sleep_time
        if sleep_time > 0:
            time.sleep(sleep_time)

        ctr += 1

send_packets(False)
"""
print "Sending dummy packets: ...."
send_dummy_packets(0)
send_dummy_packets(1)
time.sleep(1)
"""