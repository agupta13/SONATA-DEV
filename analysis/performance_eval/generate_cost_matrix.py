#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import time
import datetime
import math
import json

from sonata.query_engine.sonata_queries import *
from sonata.core.training.utils import create_spark_context
from sonata.core.integration import Target
from sonata.core.refinement import get_refined_query_id, Refinement
from sonata.core.training.hypothesis.hypothesis import Hypothesis
# from training_queries import get_training_query
from sonata.system_config import BASIC_HEADERS

"""
Schema:
ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
"""


def parse_log_line(logline):
    return tuple(logline.split(","))


def generate_cost_matrix():
    TD_PATH = '/mnt/dirAB.out_00000_20160121080100.transformed.csv/part-00000'
    flows_File = TD_PATH
    qids = [1]
    sc = create_spark_context()

    with open('sonata/config.json') as json_data_file:
        data = json.load(json_data_file)

    conf = data["on_server"][data["is_on_server"]]["sonata"]
    refinement_keys = conf["refinement_keys"]
    print "Possible Refinement Keys", refinement_keys

    target = Target()

    for qid in qids:
        query, training_data = get_training_query(sc, flows_File, qid)
        print query.qid, query.basic_headers
        refinement_object = Refinement(query, target, refinement_keys)
        print refinement_object.qid_2_refined_queries

        print "Collecting the training data for the first time ...", training_data.take(2)
        training_data = sc.parallelize(training_data.collect())
        print "Collecting timestamps for the experiment ..."
        timestamps = training_data.map(lambda s: s[0]).distinct().collect()
        print "#Timestamps: ", len(timestamps)

        refinement_object.update_filter(training_data)

        hypothesis = Hypothesis(query, sc, training_data, timestamps, refinement_object, target)

        # G = hypothesis.G
        # fname = 'data/hypothesis_graph_'+str(query.qid)+'_'+str(datetime.datetime.fromtimestamp(time.time()))+'.pickle'
        #
        # # dump the hypothesis graph: {ts:G[ts], ...}
        # print "Dumping graph to", fname
        # with open(fname, 'w') as f:
        #     pickle.dump(G, f)


def get_training_query(sc, flows_File, qid):
    q = None
    training_data = None

    T = 1  # T = 1 ==> 1 second window

    if qid == 1:
        # dupacks
        training_data = (sc.textFile(flows_File)
                         .map(parse_log_line)
                         # .map(lambda s: tuple([int(math.ceil(int(s[0]) / T))] + (list(s[1:]))))
                         .map(lambda s: tuple([1] + (list(s[1:]))))
                         .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                        tcp_flags): str(proto) == '6' and str(tcp_flags) == '16')
                         )

        q = (PacketStream(qid)
             .map(keys=('ipv4_dstIP', 'ipv4_srcIP', 'tcp_ack'), map_values=('count',), func=('eq', 1,))
             .reduce(keys=('ipv4_dstIP', 'ipv4_srcIP', 'tcp_ack'), func=('sum',))
             .filter(filter_vals=('count',), func=('geq', '99.9'))
             # .map(keys=('ipv4_dstIP',))
             )

    elif qid == 2:
        # number new connections
        training_data = (sc.textFile(flows_File)
                         .map(parse_log_line)
                         .map(lambda s: tuple([1] + (list(s[1:]))))
                         .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                        tcp_flags): str(proto) == '6' and str(tcp_flags) == '2')
                         )

        q = (PacketStream(qid)
             .map(keys=('ipv4_dstIP',), map_values=('count',), func=('eq', 1,))
             .reduce(keys=('ipv4_dstIP',), func=('sum',))
             .filter(filter_vals=('count',), func=('geq', '99.9'))
             # .map(keys=('ipv4_dstIP',))
             )

    elif qid == 3:
        # ssh bruteforce
        training_data = (sc.textFile(flows_File)
                         .map(parse_log_line)
                         .map(lambda s: tuple([1] + (list(s[1:]))))
                         .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                        tcp_flags): str(proto) == '6')
                         .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                        tcp_flags): str(dPort) == '22')
                         .map(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                                              tcp_flags): (ts, sIP, sPort, dIP, dPort,
                                                                           10 * math.ceil(float(nBytes) / 10),
                                                                            proto, tcp_seq, tcp_ack, tcp_flags))
                         )

        q = (PacketStream(qid)
             .map(keys=('ipv4_dstIP','ipv4_srcIP', 'nBytes'))
             .distinct(keys=('ipv4_dstIP','ipv4_srcIP', 'nBytes'))
             .map(keys=('ipv4_dstIP','nBytes',), map_values=('count',), func=('eq', 1,))
             .reduce(keys=('ipv4_dstIP','nBytes',), func=('sum',))
             .filter(filter_vals=('count',), func=('geq', '99.9'))
             # .map(keys=('ipv4_dstIP',))
             )

    elif qid == 4:
        # heavy hitter
        training_data = (sc.textFile(flows_File)
                         .map(parse_log_line)
                         .map(lambda s: tuple([1] + (list(s[1:]))))
                         .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                        tcp_flags): str(proto) == '6')
                         .map(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                     tcp_flags): (ts, sIP, sPort, dIP, dPort,
                                                  int(nBytes),
                                                  proto, tcp_seq, tcp_ack, tcp_flags))
                         )

        q = (PacketStream(qid)
              # .filter(filter_keys=('proto',), func=('eq', 6))
             .map(keys=('ipv4_dstIP', 'ipv4_srcIP','nBytes'))
              .map(keys=('ipv4_dstIP', 'ipv4_srcIP'), values=('nBytes',))
              .reduce(keys=('ipv4_dstIP', 'ipv4_srcIP',), func=('sum',))
              .filter(filter_vals=('nBytes',), func=('geq', '99'))
              )

    elif qid == 5:
        # super spreader
        training_data = (sc.textFile(flows_File)
                         .map(parse_log_line)
                         .map(lambda s: tuple([1] + (list(s[1:]))))
                         .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                        tcp_flags): str(proto) == '17')
                         )

        q = (PacketStream(qid)
             .map(keys=('ipv4_dstIP', 'ipv4_srcIP'))
             .distinct(keys=('ipv4_dstIP', 'ipv4_srcIP'))
             .map(keys=('ipv4_srcIP',), map_values=('count',), func=('eq', 1,))
             .reduce(keys=('ipv4_srcIP',), func=('sum',))
             .filter(filter_vals=('count',), func=('geq', '99.9'))
             # .map(keys=('ipv4_dstIP',))
             )

    elif qid == 6:
        # port scan
        training_data = (sc.textFile(flows_File)
                         .map(parse_log_line)
                         .map(lambda s: tuple([1] + (list(s[1:]))))
                         )

        q = (PacketStream(qid)
                     # .filter(filter_keys=('proto',), func=('eq', 6))
                     .map(keys=('ipv4_srcIP', 'dPort'))
                     .distinct(keys=('ipv4_srcIP', 'dPort'))
                     .map(keys=('ipv4_srcIP',), map_values=('count',), func=('eq', 1,))
                     .reduce(keys=('ipv4_srcIP',), func=('sum',))
                     .filter(filter_vals=('count',), func=('geq', '99.99'))
                     )

    elif qid == 7:

        training_data = (sc.textFile(flows_File)
                         .map(parse_log_line)
                         # .map(lambda s: tuple([int(math.ceil(int(s[0]) / T))] + (list(s[1:]))))
                         .map(lambda s: tuple([1] + (list(s[1:]))))
                         .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, tcp_seq, tcp_ack,
                                        tcp_flags): str(proto) == '17')
                         )

        q = (PacketStream(qid)
             .map(keys=('ipv4_dstIP', 'ipv4_srcIP'))
             .distinct(keys=('ipv4_dstIP', 'ipv4_srcIP'))
             .map(keys=('ipv4_dstIP',), map_values=('count',), func=('eq', 1,))
             .reduce(keys=('ipv4_dstIP',), func=('sum',))
             .filter(filter_vals=('count',), func=('geq', '99.9'))
             # .map(keys=('ipv4_dstIP',))
             )

    q.basic_headers = BASIC_HEADERS

    return q, training_data


if __name__ == '__main__':
    generate_cost_matrix()