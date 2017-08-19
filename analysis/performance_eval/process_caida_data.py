#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import time
import datetime
import math
import json

from sonata.query_engine.sonata_queries import *
from sonata.core.training.utils import get_spark_context_batch, create_spark_context
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


def reduce_size(fname, out_fname):
    sc = create_spark_context()
    """
    ts,sIP,sPort,dIP,dPort,nBytes,proto,sMac,dMac,tcp.seq,tcp.ack,tcp.flags,tcp.flags.ns,tcp.flags.cwr,tcp.flags.ecn,
    tcp.flags.urg,tcp.flags.ack,tcp.flags.push,tcp.flags.reset,tcp.flags.syn,tcp.flags.fin,tcp.hdr_len,tcp.window_size
    """
    transformed_data = (sc.textFile(fname)
                        .map(parse_log_line)
                        )
    print transformed_data.take(5)

    transformed_data = (transformed_data
                        .map(lambda s: tuple([str(s[0]).split(".")[0]]+list(s[1:7]) + get_tcp_flag(s[11])))
                        )
    print transformed_data.take(5)

    """
    ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.flags
    """

    lines = transformed_data.map(toCSVLine)
    lines.saveAsTextFile(out_fname)


if __name__ == '__main__':
    fname = "/mnt/dirAB.out_00000_20160121080100.pcap.csv"
    # fname = "/mnt/caida_test.csv"
    out_fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv"
    reduce_size(fname, out_fname)
