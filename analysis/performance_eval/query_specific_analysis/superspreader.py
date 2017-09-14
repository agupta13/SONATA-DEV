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
from sonata.core.utils import dump_rdd, load_rdd, TMP_PATH, parse_log_line
from sonata.core.utils import *


def analyse_query(fname):
    sc = create_spark_context()

    # packets = (sc.textFile(fname)
    #            .map(parse_log_line)
    #            .map(lambda s: tuple([1] + (list(s[1:]))))
    #            )

    training_data_fname = "/mnt/tmp/training_data"
    out = (load_rdd(training_data_fname, sc)
           .map(lambda ((ts,ipv4_srcIP,sPort,ipv4_dstIP,dPort,nBytes,proto,tcp_seq,tcp_ack,tcp_flags)): ((ipv4_dstIP,
                                                                                                          str(IPNetwork(str(str(ipv4_srcIP)+"/32")).network),ts)))
           .map(lambda ((ipv4_dstIP,ipv4_srcIP,ts)): ((ts,ipv4_dstIP,ipv4_srcIP)))
           .distinct().map(lambda ((ts,ipv4_dstIP,ipv4_srcIP)): ((ts,ipv4_srcIP),(1)))
           .reduceByKey(lambda x,y: x+y).map(lambda s: s[1]).collect())


    """
    Schema:
    ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
    """



if __name__ == '__main__':
    fname = "/mnt/data/1301_transformed/*.csv"
    analyse_query(fname)
