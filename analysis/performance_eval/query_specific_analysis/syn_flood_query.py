#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import time
import datetime
import math
import json
import numpy as np
from netaddr import *

from sonata.query_engine.sonata_queries import *
from sonata.core.training.utils import create_spark_context
from sonata.core.integration import Target
from sonata.core.refinement import get_refined_query_id, Refinement
from sonata.core.training.hypothesis.hypothesis import Hypothesis


def parse_log_line(logline):
    return tuple(logline.split(","))


def get_diff(s):
    if s[1][1] is None:
        return tuple([s[0], s[1][0]])
    else:
        return tuple([s[0], s[1][0] - 2*s[1][1]])


def get_sum(s):
    if s[1][1] is None:
        return tuple([s[0], s[1][0]])
    else:
        return tuple([s[0], s[1][0] + s[1][1]])


def analyse_query(fname):
    sc = create_spark_context()

    packets = (sc.textFile(fname)
               .map(parse_log_line)
               # set ts = 1 for all packets
               .map(lambda s: tuple([1] + (list(s[1:]))))
               # .filter(lambda s: str(s[3]) == '43.236.76.200')
               )

    """
    Schema:
    s,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
    """

    n_syn = (packets
             .filter(lambda s: str(s[-4]) == '6')
             .filter(lambda s: str(s[-1]) == '2')
             # .filter(lambda s: str(s[3]) == '122.226.186.143')
             .map(lambda s: (str(IPNetwork(str(str(s[3])+"/"+str(16))).network), 1))
             .reduceByKey(lambda x, y: x + y)
             )
    print n_syn.take(5), n_syn.count()

    n_synack = (packets
                .filter(lambda s: str(s[-4]) == '6')
                .filter(lambda s: str(s[-1]) == '17')
                # .filter(lambda s: str(s[1]) == '122.226.186.143')
                .map(lambda s: (str(IPNetwork(str(str(s[1])+"/"+str(16))).network), 1))
                .reduceByKey(lambda x, y: x + y)
                )
    print n_synack.take(5), n_synack.count()

    n_ack = (packets
             .filter(lambda s: str(s[-4]) == '6')
             .filter(lambda s: str(s[-1]) == '16')
             # .filter(lambda s: str(s[3]) == '122.226.186.143')
             .map(lambda s: (str(IPNetwork(str(str(s[3])+"/"+str(16))).network), 1))
             .reduceByKey(lambda x, y: x + y)
             )
    print n_ack.take(5), n_ack.count()

    Th = 1000


    victim = (n_syn
              .join(n_synack)
              .map(lambda s: get_sum(s))
              .leftOuterJoin(n_ack)
              .map(lambda s: get_diff(s))
              # .filter(lambda s: s[1] > Th)
              )

    data = victim.map(lambda s: s[1]).collect()
    print "Mean", np.mean(data), \
        "Median", np.median(data), \
        "75 %", np.percentile(data, 75), \
        "95 %", np.percentile(data, 95), \
        "99 %", np.percentile(data,99), \
        "99.9 %", np.percentile(data, 99.9)
    print max(data)

    print victim.take(10), victim.count()


if __name__ == '__main__':
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv/part-00000"
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv"
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv/part-00000"
    analyse_query(fname)
