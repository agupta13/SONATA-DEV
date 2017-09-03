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
# However, for fair comparison we will only use keys that are required for aggregation.
query_2_keys = {1: ['dIP', 'sIP', 'tcp_ack'], 2: ['dIP'],
                3: ['dIP', 'sIP', 'nBytes'], 4: ['dIP', 'sIP'], 5: ['dIP', 'sIP'],
                6: ['sIP', 'dPort'], 7: ['dIP', 'sIP'], 8: ['dIP', 'sIP'], 91: ['dIP'], 92: ['dIP'], 93: ['dIP'],
                101: ['dIP'], 102: ['dIP'], 111: ['dIP', 'sPort'], 112: ['sIP', 'dPort'], 121: ['dIP', 'sIP', 'sPort'],
                122: ['dIP']
                }

query_2_filter = {1: "str(packet[-1])=='16'", 2: "str(packet[-1])=='2'",
                  3: "str(packet[4])=='22'", 4: "str(packet[6])=='6'", 5: "", 6: "", 7: "str(packet[6])=='17'",
                  8: "str(packet[6])=='6'", 91: "str(packet[-1])=='2'", 92: "str(packet[-1])=='17'",
                  93: "str(packet[-1])=='16'", 101: "str(packet[-1])=='2'", 102: "str(packet[-1])=='1'",
                  111: "str(packet[6])=='17'", 112: "str(packet[6])=='17'", 121: "str(packet[6])=='6'",
                  122: "str(packet[6])=='6'"
                  }

memory_sizes = [500000, 1000000, 2000000, 4000000, 8000000]
# memory_sizes = [80000]
packet_outs = {}


def get_ordered_packets(total_packets_fname, sc, qid):
    total_packets = load_rdd(total_packets_fname, sc)
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

# clean the tmp directory before running the experiment
clean_cmd = "rm -rf " + TMP_PATH + "*"
# print "Running command", clean_cmd
os.system(clean_cmd)

total_packets = (sc.textFile(flows_File).map(parse_log_line))
total_packets_fname = "total_packets"
dump_rdd(total_packets_fname, total_packets)
total_packets = None
print "Collected All packets"

for qid in query_2_keys:
    packet_outs[qid] = {}
    ordered_packets = get_ordered_packets(total_packets_fname, sc, qid)
    print "Processing", len(ordered_packets), "packets for query", qid
    for size in memory_sizes:
        # size = size/10
        print "Size", size
        cache = LRUCache(size, query_2_keys[qid])
        total_in = 0
        for packet in ordered_packets:
            cache.process_packet(packet)
            total_in += 1

        cached_entries = len(cache.lru.items())
        total_out = cache.out_packets + cached_entries

        print "Cache Size", cache.cache_size
        print "Total In Packets", total_in
        print "Total Evicted Packets", cache.out_packets
        print "Total keys in cache", cached_entries
        print "Total Out Packets", total_out
        packet_outs[qid][size] = (total_out, total_in)

tmp = "-".join(str(datetime.datetime.fromtimestamp(time.time())).split(" "))
cost_fname = 'data/query_cost_lru_' + tmp + '.pickle'
print packet_outs
print "Dumping data to file", cost_fname, "..."
with open(cost_fname, 'w') as f:
    pickle.dump(packet_outs, f)
