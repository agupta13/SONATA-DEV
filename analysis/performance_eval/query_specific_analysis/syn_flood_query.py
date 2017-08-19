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
    ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.flags
    """

    n_syn = (packets
             .filter(lambda s: str(s[-2]) == '6')
             .filter(lambda s: str(s[-1]) == '2')
             .map(lambda s: (s[3], 1))
             .reduceByKey(lambda x, y: x + y)
             )
    print n_syn.take(5), n_syn.count()

    n_synack = (packets
                .filter(lambda s: str(s[-2]) == '6')
                .filter(lambda s: str(s[-1]) == '17')
                .map(lambda s: (s[3], 1))
                .reduceByKey(lambda x, y: x + y)
                )
    print n_synack.take(5), n_synack.count()

    n_ack = (packets
             .filter(lambda s: str(s[-2]) == '6')
             .filter(lambda s: str(s[-1]) == '16')
             .map(lambda s: (s[3], 1))
             .reduceByKey(lambda x, y: x + y)
             )
    print n_ack.take(5), n_ack.count()

    Th = 5

    victim = (n_syn
              .join(n_synack)
              .map(lambda s: (s[0], sum(list(s[1]))))
              .join(n_ack)
              .map(lambda s: (s[0], s[1][0] - s[1][1]))
              .filter(lambda s: s[1] > Th)
              )

    print victim.take(10), victim.count()


if __name__ == '__main__':
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv"
    analyse_query(fname)
