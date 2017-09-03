from lru import LRU
import math

"""
Schema:
ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp_seq,tcp_ack,tcp_flags
Older schema
ts, sIP, sPort, dIP, dPort, nBytes, proto, sMac, dMac, tcp_seq, tcp_ack, ..., tcp_flags
"""


class LRUCache(object):
    out_packets = 0
    key_2_index = {'ts': 0, 'sIP': 1, 'sPort': 2, 'dIP': 3, 'dPort': 4, 'nBytes': 5, 'proto': 6, 'tcp_seq': 7,
                   'tcp_ack': 8, 'tcp_flags': -1}
    keys_2_bits = {'ts': 0, 'sIP': 32, 'sPort': 16, 'dIP': 32, 'dPort': 16, 'nBytes': 32, 'proto': 8, 'tcp_seq': 32,
                   'tcp_ack': 32, 'tcp_flags': 8}
    five_tuple_size = 104
    reduction_key_size = 0
    counter_size = 32
    cache_size = 0

    def __init__(self, size, reduction_keys=list()):
        self.size = size
        self.reduction_keys = reduction_keys
        self.update_reduction_key_size()
        self.compute_cache_size()
        self.lru = LRU(self.cache_size, callback=self.evicted)


    def evicted(self, key, value):
        # print "evicting key: %s" % (key)
        self.out_packets += 1

    def update_reduction_key_size(self):
        for red_key in self.reduction_keys:
            self.reduction_key_size += self.keys_2_bits[red_key]

    def compute_cache_size(self):
        self.cache_size = int(math.floor(float(self.size)/(self.counter_size+self.reduction_key_size)))

    def process_packet(self, packet):
        k = ",".join([str(packet[self.key_2_index[red_key]]) for red_key in self.reduction_keys])
        self.lru[k] = 1


if __name__ == '__main__':
    l = LRUCache(200, ["dIP", "sIP", "dPort"])
    packets = [(1453381260, "43.252.226.27", 80, "125.111.31.157", 60683, 1450, 6, 1, 1, 16),
               (1453381260, "43.252.226.27", 80, "125.111.31.158", 60683, 1450, 6, 1399, 1, 16),
               (1453381260, "146.2.215.244", 443, "131.211.254.227", 57591, 83, 6, 1, 1, 24),
               (1453381260, "155.138.100.149", 80, "13.166.237.70", 3019, 1500, 6, 1, 1, 16),
               (1453381260, "43.252.226.27", 80, "125.111.31.159", 60683, 1450, 6, 2797, 1, 16)]

    for packet in packets:
        l.process_packet(packet)

    print l.lru.items()
    print "Cache Size", l.cache_size
    print "Total In Packets", len(packets)
    print "Total Out Packets", l.out_packets
