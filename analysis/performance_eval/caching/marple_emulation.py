from analysis.performance_eval.caching.lru_cache import *

query_2_keys = {8: ['dIP', 'sIP']}

# fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv/part-00000"
fname0 = "/mnt/caida_20160121080147/dirAB.out_00000_20160121080100.pcap.csv"
fname1 = "/mnt/caida_20160121080147/dirAB.out_00001_20160121080100.pcap.csv"
fname2 = "/mnt/caida_20160121080147/dirAB.out_00002_20160121080100.pcap.csv"
fname3 = "/mnt/caida_20160121080147/dirAB.out_00003_20160121080100.pcap.csv"
fname4 = "/mnt/caida_20160121080147/dirAB.out_00004_20160121080100.pcap.csv"
fname5 = "/mnt/caida_20160121080147/dirAB.out_00005_20160121080100.pcap.csv"
fname6 = "/mnt/caida_20160121080147/dirAB.out_00006_20160121080100.pcap.csv"
fname7 = "/mnt/caida_20160121080147/dirAB.out_00007_20160121080100.pcap.csv"
fname8 = "/mnt/caida_20160121080147/dirAB.out_00008_20160121080100.pcap.csv"
fname9 = "/mnt/caida_20160121080147/dirAB.out_00009_20160121080100.pcap.csv"
fname10 = "/mnt/caida_20160121080147/dirAB.out_00010_20160121080100.pcap.csv"

fnames_ordered = [fname0, fname1, fname2, fname3, fname4, fname5, fname6, fname7, fname8, fname9, fname10]

# memory_sizes = [1000000, 2000000, 4000000, 8000000]
memory_sizes = [8000000]
packet_outs = {}

for size in memory_sizes:
    packet_outs[size] = {}
    for qid in query_2_keys:
        l = LRUCache(size, query_2_keys[qid])
        total_in = 0
        for fname in fnames_ordered:
            with open(fname, 'r') as f:
                for line in f:
                    packet = line.split("\n")[0].split(",")
                    l.process_packet(packet)
                    total_in += 1
            # break
        print "Cache Size", l.cache_size
        print "Total In Packets", total_in
        print "Total Out Packets", l.out_packets
        packet_outs[size][qid] = (l.out_packets, total_in)

print packet_outs
