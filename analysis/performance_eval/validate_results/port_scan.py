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
    out = (packets
             .map(lambda s: (str(IPNetwork(str(str(s[1])+"/8")).network), s[4]))
             .distinct()

             )
    print out.take(2)
    count = out.count()
    assert(count == 522682)




if __name__ == '__main__':
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv"

    TD_PATH = '/mnt/caida_20160121080147_transformed'

    baseDir = os.path.join(TD_PATH)
    flows_File = os.path.join(baseDir, '*.csv')

    analyse_query(flows_File)
