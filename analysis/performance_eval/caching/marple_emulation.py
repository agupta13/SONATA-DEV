import time
import datetime
import pickle
import os

from analysis.performance_eval.caching.lru_cache import *
from sonata.core.training.utils import create_spark_context
from sonata.core.utils import dump_rdd, load_rdd, TMP_PATH, parse_log_line

"""
Schema:
ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp_seq,tcp_ack,tcp_flags
Older schema
ts, sIP, sPort, dIP, dPort, nBytes, proto, sMac, dMac, tcp_seq, tcp_ack, ..., tcp_flags
"""

# fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv/part-00000"
fname0 = "/mnt/caida_20160121080147/dirAB.out_00000_20160121080100.pcap.csv"
fname1 = "/mnt/caida_20160121080147/dirAB.out_00001_20160121080105.pcap.csv"
fname2 = "/mnt/caida_20160121080147/dirAB.out_00002_20160121080110.pcap.csv"
fname3 = "/mnt/caida_20160121080147/dirAB.out_00003_20160121080116.pcap.csv"
fname4 = "/mnt/caida_20160121080147/dirAB.out_00004_20160121080121.pcap.csv"
fname5 = "/mnt/caida_20160121080147/dirAB.out_00005_20160121080131.pcap.csv"
fname6 = "/mnt/caida_20160121080147/dirAB.out_00006_20160121080136.pcap.csv"
fname7 = "/mnt/caida_20160121080147/dirAB.out_00007_20160121080142.pcap.csv"
fname8 = "/mnt/caida_20160121080147/dirAB.out_00008_20160121080147.pcap.csv"
fname9 = "/mnt/caida_20160121080147/dirAB.out_00009_20160121080153.pcap.csv"
fname10 = "/mnt/caida_20160121080147/dirAB.out_00010_20160121080158.pcap.csv"

fnames_ordered = [fname0, fname1, fname2, fname3, fname4, fname5, fname6, fname7, fname8, fname9, fname10]

# Note that Marple uses 5 tuples for aggregation irrespective of actual keys required for aggregation
# Figure 9 says each k,v pair uses 32+104 bits even though it is aggregating over srcIP
query_2_keys = {8: ['dIP', 'sIP'], 1: ['dIP', 'sIP', 'tcp_ack'], 2: ['dIP'],
                3: ['dIP', 'sIP', 'nBytes'], 4: ['dIP', 'sIP'], 5: ['dIP', 'sIP'],
                6: ['sIP', 'dPort'], 7: ['dIP', 'sIP']}

query_2_filter = {8: "str(packet[6])=='6'", 1: "str(packet[-1])=='16'", 2: "str(packet[-1])=='2'",
                  3: "str(packet[4])=='22'", 4: "str(packet[6])=='6'", 5: "", 6: "", 7: "str(packet[6])=='17'"}

memory_sizes = [1000000, 2000000, 4000000, 8000000]
# memory_sizes = [80000]
packet_outs = {}


def get_ordered_packets(total_packets, qid):
    if len(query_2_filter[qid]) > 0:
        packets = (total_packets
                   .filter(lambda packet: eval(query_2_filter[qid]))
                   )
    else:
        packets = total_packets

    return packets.collect()


TD_PATH = '/mnt/caida_20160121080147_transformed'

baseDir = os.path.join(TD_PATH)
flows_File = os.path.join(baseDir, '*.csv')
sc = create_spark_context()
# flows_File = '/mnt/caida_20160121080147_transformed/dirAB.out_00000_20160121080100.pcap.csv/part-00000'
total_packets = (sc.textFile(flows_File).map(parse_log_line).cache())
print "Collected All packets"

for qid in query_2_keys:
    packet_outs[qid] = {}
    ordered_packets = get_ordered_packets(total_packets, qid)
    print "Processing", len(ordered_packets), "packets for query", qid
    for size in memory_sizes:
        print "Size", size
        cache = LRUCache(size, query_2_keys[qid])
        total_in = 0
        for packet in ordered_packets:
            cache.process_packet(packet)
            total_in += 1

        print "Cache Size", cache.cache_size
        print "Total In Packets", total_in
        print "Total Out Packets", cache.out_packets
        packet_outs[qid][size] = (cache.out_packets, total_in)

tmp = "-".join(str(datetime.datetime.fromtimestamp(time.time())).split(" "))
cost_fname = 'data/query_cost_lru_' + '_' + tmp + '.pickle'
print packet_outs
with open(cost_fname, 'w') as f:
    print "Dumping data to file", cost_fname, "..."
    # pickle.dump(packet_outs, f)
