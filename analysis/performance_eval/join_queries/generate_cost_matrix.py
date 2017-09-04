#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import time
import datetime
import math
import json
import os
import numpy as np
import math
from netaddr import *

from sonata.query_engine.sonata_queries import *
from sonata.core.training.utils import create_spark_context
from sonata.core.integration import Target
from sonata.core.refinement import get_refined_query_id, Refinement
from sonata.core.training.hypothesis.hypothesis import Hypothesis
from sonata.system_config import BASIC_HEADERS
from sonata.core.utils import dump_rdd, load_rdd, TMP_PATH, parse_log_line

import analysis.performance_eval.join_queries.syn_flood as q9
import analysis.performance_eval.join_queries.completed_flows as q10
import analysis.performance_eval.join_queries.traffic_asymmetry as q11
import analysis.performance_eval.join_queries.slowloris as q12


def generate_counts(data_fname):
    qids = [9, 10, 11, 12]
    # qids = [9]
    qid_2_modules = {9: q9, 10: q10, 11: q11, 12: q12}

    sc = create_spark_context()

    for qid in qids:
        # clean the tmp directory before running the experiment
        clean_cmd = "rm -rf " + TMP_PATH + "*"
        # print "Running command", clean_cmd
        os.system(clean_cmd)

        print "## Getting costs for query", qid

        tmp = "-".join(str(datetime.datetime.fromtimestamp(time.time())).split(" "))
        cost_fname = 'data/query_cost_transit_' + str(qid) + '_' + tmp + '.pickle'
        query_count_transit = qid_2_modules[qid].analyse_query(data_fname, sc)

        with open(cost_fname, 'w') as f:
            print "Dumping costs into pickle", cost_fname, "..."
            pickle.dump(query_count_transit, f)


if __name__ == '__main__':
    TD_PATH = '/mnt/caida_20160121080147_transformed'
    baseDir = os.path.join(TD_PATH)
    fname = os.path.join(baseDir, '*.csv')

    # fname = "/mnt/caida_20160121080147_transformed/dirAB.out_00000_20160121080100.pcap.csv/part-00000"

    generate_counts(fname)
