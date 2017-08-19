#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import time
import datetime
import math
import json
import numpy as np

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
    s,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
    """

    n_syn = (packets
             .filter(lambda s: str(s[-4]) == '6')
             .filter(lambda s: str(s[-1]) == '2')
             .map(lambda s: (s[3], 1))
             .reduceByKey(lambda x, y: x + y)
             )
    print n_syn.take(5), n_syn.count()

    n_fin = (packets
             .filter(lambda s: str(s[-4]) == '6')
             .filter(lambda s: str(s[-1]) == '1')
             .map(lambda s: (s[3], 1))
             .reduceByKey(lambda x, y: x + y)
             )
    print n_fin.take(5), n_fin.count()

    Th = 0

    victim = (n_fin
              .join(n_syn)
              .map(lambda s: (s[0], s[1][0] - s[1][1]))
              .filter(lambda s: s[1] >= Th)
              )

    print victim.take(10), victim.count()
    data = victim.map(lambda s: s[1]).collect()
    if len(data) > 0:
        print "Total Keys", len(data), "max", max(data), "min", min(data)
        print "Mean", np.mean(data), "Median", np.median(data), "75 %", np.percentile(data, 75), \
            "95 %", np.percentile(data, 95), "99 %", np.percentile(data, 99), "99.9 %", np.percentile(data, 99.9)


if __name__ == '__main__':
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv"
    analyse_query(fname)
