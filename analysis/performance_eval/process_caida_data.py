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


def parse_log_line(logline):
    return tuple(logline.split(","))


def toCSVLine(data):
    return ','.join(str(d) for d in data)


def get_tcp_flag(entry):
    entry = str(entry)
    out = []
    if '0x' in entry:
        out.append(int(entry, 16))
    else:
        out.append('')
    return out


def reduce_size(sc, fname, out_fname):

    """
    ts,sIP,sPort,dIP,dPort,nBytes,proto,sMac,dMac,tcp.seq,tcp.ack,tcp.flags,tcp.flags.ns,tcp.flags.cwr,tcp.flags.ecn,
    tcp.flags.urg,tcp.flags.ack,tcp.flags.push,tcp.flags.reset,tcp.flags.syn,tcp.flags.fin,tcp.hdr_len,tcp.window_size
    """
    transformed_data = (sc.textFile(fname)
                        .map(parse_log_line)
                        )
    # print transformed_data.take(5)

    transformed_data = (transformed_data
                        .map(lambda s: tuple([str(s[0]).split(".")[0]]+list(s[1:7]) + list(s[9:11]) + get_tcp_flag(s[11])))
                        )
    # print transformed_data.take(5)

    """
    ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
    """

    lines = transformed_data.map(toCSVLine)
    lines.saveAsTextFile(out_fname)


if __name__ == '__main__':
    in_dir = "/mnt/caida_20160121080147/"
    out_dir = "/mnt/caida_20160121080147_transformed/"
    sc = create_spark_context()

    fnames = ['dirAB.out_00000_20160121080100.pcap.csv',
              'dirAB.out_00006_20160121080136.pcap.csv',
              'dirAB.out_00001_20160121080105.pcap.csv',
              'dirAB.out_00007_20160121080142.pcap.csv',
              'dirAB.out_00002_20160121080110.pcap.csv',
              'dirAB.out_00008_20160121080147.pcap.csv',
              'dirAB.out_00003_20160121080116.pcap.csv',
              'dirAB.out_00009_20160121080153.pcap.csv',
              'dirAB.out_00004_20160121080121.pcap.csv',
              'dirAB.out_00010_20160121080158.pcap.csv'
              ]

    for fname in fnames:
        in_fname = in_dir+fname
        out_fname = out_dir+fname
        print "Transforming file", fname
        reduce_size(sc, in_fname, out_fname)
