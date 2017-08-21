#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import time
import datetime
import math
import json
import os
from netaddr import *

from sonata.query_engine.sonata_queries import *
from sonata.core.training.utils import create_spark_context
from sonata.core.integration import Target
from sonata.core.refinement import get_refined_query_id, Refinement
from sonata.core.training.hypothesis.hypothesis import Hypothesis


def parse_log_line(logline):
    return tuple(logline.split(","))


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
    ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
    """

    # check if the automated query generator is estimating
    # the correct value for port scan query at /8 and distinct operator
    filtered_packets = packets.filter(lambda s: int(s[6]) == 17).cache()

    out8_distinct_only = (filtered_packets
            .map(lambda s: (str(IPNetwork(str(str(s[3]) + "/8")).network), s[1]))
            .distinct()
            )

    print out8_distinct_only.take(2)
    print out8_distinct_only.count()

    out8 = (filtered_packets
            .map(lambda s: (str(IPNetwork(str(str(s[3]) + "/8")).network), s[1]))
            .distinct()
            .map(lambda s: (s[0], 1))
            .reduceByKey(lambda x, y: x + y)
            .filter(lambda s: s[1] >= 134.0)
            )
    print out8.take(2)
    count = out8.count()
    print count
    # assert (count == 522682)

    out8_32 = (filtered_packets
            .map(lambda s: ((str(IPNetwork(str(str(s[3]) + "/8")).network), s)))
             .join(out8)
             .map(lambda s: s[1][0])
             )

    print out8_32.take(2)
    print out8_32.count()


if __name__ == '__main__':
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv"

    TD_PATH = '/mnt/caida_20160121080147_transformed'

    baseDir = os.path.join(TD_PATH)
    flows_File = os.path.join(baseDir, '*.csv')

    analyse_query(flows_File)
