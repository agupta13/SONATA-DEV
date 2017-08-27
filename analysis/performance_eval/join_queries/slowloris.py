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


def parse_log_line(logline):
    return tuple(logline.split(","))


def get_filter_query(packets_fname, qid, ref_level, Th=[]):
    out = ''
    if qid == 121:
        out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), 1))' \
              '.reduceByKey(lambda x, y: x + y))' % (str(ref_level))

    elif qid == 123:
        out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), 1))' \
              '.reduceByKey(lambda x, y: x + y)).filter(lambda s: s[1] > %d).join(' \
              'load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), int(s[5])))' \
              '.reduceByKey(lambda x, y: x + y))' \
              '.map(lambda s: (s[0], float(s[1][0])/float(s[1][1])))' % (str(ref_level), Th[0], str(ref_level))

    return out


def get_spark_query(packets_fname, qid, ref_level, part, Th=[]):
    out = ''
    if qid == 122:
        if part == 0:
            out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[1])+"/%s")).network),s[1], s[2]),  int(s[5]))))' % (
                str(ref_level))

        else:
            out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[1])+"/%s")).network), s[1], s[2]),  int(s[5]))).reduceByKey(lambda x, y: x + y))' % (
                str(ref_level))
    elif qid == 121:
        if part == 0:
            out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), 1)))' % (
                str(ref_level))

        elif part == 2:
            out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), 1))' \
                  '.reduceByKey(lambda x, y: x + y))' % (str(ref_level))

        elif part == 3:
            out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), 1))' \
                  '.reduceByKey(lambda x, y: x + y).filter(lambda s: s[1] >= %d))' % (str(ref_level), Th[0])

    elif qid == 123:
        out = '(load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), 1))' \
              '.reduceByKey(lambda x, y: x + y)).filter(lambda s: s[1] >= %d).join(' \
              'load_rdd(packets_fname, sc).map(lambda s: ((s[0], str(IPNetwork(str(str(s[3])+"/%s")).network), s[1], s[2]), int(s[5])))' \
              '.reduceByKey(lambda x, y: x + y))' \
              '.map(lambda s: (s[0], float(s[1][0])/float(s[1][1]))).filter(lambda s: s[1]>= %d)' % (str(ref_level), Th[0],
                                                                                       str(ref_level), Th[1])

    return out


def analyse_query(fname):

    # clean the tmp directory before running the experiment
    clean_cmd = "rm -rf " + TMP_PATH + "*"
    # print "Running command", clean_cmd
    os.system(clean_cmd)

    final_query_out = {}

    sc = create_spark_context()

    packets = (sc.textFile(fname)
               .map(parse_log_line)
               # set ts = 1 for all packets
               .map(lambda s: tuple([1] + (list(s[1:]))))
               .filter(lambda s: str(s[-4]) == '6')
               .cache()
               )
    packets_fname = "training_data_" + str(11)
    dump_rdd(packets_fname, packets)
    """
    Schema:
    ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
    """

    partitioning_plans = {121: [0, 2, 3], 122: [0, 2], 123: [2]}
    refinement_levels = range(0, GRAN_MAX, GRAN)[1:]
    print refinement_levels
    counts = {}
    qids = [121, 122, 123]
    prev_qids = {121: 123, 122: 123, 123: 123}
    spark_queries = {}
    query_out = {}
    query_2_percentile_thresh = {121: 99.9, 123: 99.9}
    query_2_actual_thresh = {}
    refinement_levels.sort(reverse=True)

    for qid in qids:
        if qid in query_2_percentile_thresh:
            query_2_actual_thresh[qid] = {}
            for ref_level in refinement_levels:
                print "Finding Thresholds for query", qid, "level", ref_level
                if qid == 121:
                    tmp_query = get_filter_query(packets_fname, qid, ref_level)
                    # print tmp_query
                    if ref_level < refinement_levels[0]:
                        out_filter = load_rdd(final_query_out[qid], sc)
                        # print out_filter.take(5)
                        refined_satisfied_out = (out_filter
                                                 .map(
                            lambda s: ((s[0][0], str(IPNetwork(str(str(s[0][1]) + "/" + str(ref_level))).network), s[0][2], s[0][3]), 1))
                                                 .reduceByKey(lambda x, y: x + y)
                                                 )
                        # print eval(tmp_query).take(2), refined_satisfied_out.take(2)
                        data = eval(tmp_query).join(refined_satisfied_out).map(lambda s: s[1][0]).collect()
                        # print query_string
                        data = [float(x) for x in data]
                        # print data[:5]
                        thresh = 2
                        if len(data) > 0:
                            thresh = min(data)

                    else:
                        data = eval(tmp_query).map(lambda s: s[1]).collect()
                        # print data[:5]
                        thresh = 0.0
                        spread = query_2_percentile_thresh[qid]
                        if len(data) > 0:
                            thresh = float(np.percentile(data, float(spread)))
                            # print "Mean", np.mean(data), "Median", np.median(data), "75 %", np.percentile(data, 75), \
                            #     "95 %", np.percentile(data, 95), "99 %", np.percentile(data,
                            #                                                            99), "99.9 %", np.percentile(
                            #     data, 99.9)


                        filter_out_fname = "filter_out_" + str(qid) + "_" + str(ref_level)
                        post_filter_query = get_spark_query(packets_fname, qid, ref_level,
                                                            partitioning_plans[qid][-1], Th=[thresh])

                        filter_out_rdd = eval(post_filter_query)

                        # print "post_filter_query", post_filter_query, filter_out_rdd.take(2)

                        dump_rdd(filter_out_fname, filter_out_rdd)
                        final_query_out[qid] = filter_out_fname

                    # print qid, ref_level, thresh, tmp_query
                    query_2_actual_thresh[qid][ref_level] = thresh

                elif qid == 123:
                    tmp_query = get_filter_query(packets_fname, qid, ref_level,
                                                 Th=[query_2_actual_thresh[121][ref_level]])
                    # print qid, tmp_query
                    if ref_level < refinement_levels[0]:
                        out_filter = load_rdd(final_query_out[qid], sc)
                        # print out_filter.take(5)
                        refined_satisfied_out = (out_filter
                                                 .map(
                            lambda s: ((s[0][0], str(IPNetwork(str(str(s[0][1]) + "/" + str(ref_level))).network), s[0][2], s[0][3]), 1))
                                                 .reduceByKey(lambda x, y: x + y)
                                                 )
                        # print refined_satisfied_out
                        data = eval(tmp_query).join(refined_satisfied_out).map(lambda s: s[1][0]).collect()
                        # print query_string
                        data = [float(x) for x in data]
                        # print data[:5]
                        thresh = 2
                        if len(data) > 0:
                            thresh = min(data)

                    else:
                        data = eval(tmp_query).map(lambda s: s[1]).collect()
                        # print data[:5]
                        thresh = 0.0
                        spread = query_2_percentile_thresh[qid]
                        if len(data) > 0:
                            thresh = float(np.percentile(data, float(spread)))
                            # print "Mean", np.mean(data), "Median", np.median(data), "75 %", np.percentile(data, 75), \
                            #     "95 %", np.percentile(data, 95), "99 %", np.percentile(data,
                            #                                                            99), "99.9 %", np.percentile(
                            #     data, 99.9)


                        filter_out_fname = "filter_out_" + str(qid) + "_" + str(ref_level)
                        post_filter_query = get_spark_query(packets_fname, qid, ref_level, '1',
                                                            Th=[query_2_actual_thresh[121][ref_level], thresh])
                        # print post_filter_query
                        filter_out_rdd = eval(post_filter_query)
                        dump_rdd(filter_out_fname, filter_out_rdd)
                        final_query_out[qid] = filter_out_fname

                    query_2_actual_thresh[qid][ref_level] = thresh
                print "For query", qid, "level", ref_level, "thresh is", thresh

    # query_2_actual_thresh = {123: {32: 24, 16: 36.0}, 121: {32: 16, 16: 16.0}}
    print query_2_actual_thresh

    query_count_transit_fname = {}
    query_count_transit = {}
    for qid in qids:
        spark_queries[qid] = {}
        query_count_transit_fname[qid] = {}
        query_count_transit[qid] = {}
        for ref_level in refinement_levels:
            transit = (0, ref_level)
            query_count_transit_fname[qid][ref_level] = {}
            spark_queries[qid][ref_level] = {}
            query_count_transit[qid][transit] = {}
            for partid in partitioning_plans[qid]:
                if qid == 123:
                    Th = [query_2_actual_thresh[121][ref_level], query_2_actual_thresh[123][ref_level]]
                elif qid == 121:
                    Th = [query_2_actual_thresh[121][ref_level]]
                else:
                    Th = []
                tmp_query = get_spark_query(packets_fname, qid, ref_level, partid, Th)
                spark_queries[qid][ref_level][partid] = tmp_query
                out_fname = "query_count_transit_" + str(qid) + "_" + str(ref_level) + "_" + str(
                    transit[0]) + "_" + str(transit[1]) + "_" + str(partid)
                # print qid, transit, partid, tmp_query
                tmp_transit_rdd = eval(tmp_query)
                dump_rdd(out_fname, tmp_transit_rdd)
                query_count_transit_fname[qid][ref_level][partid] = out_fname

                tmp_count = (tmp_transit_rdd
                             .map(lambda s: (s[0][0], 1))
                             .reduceByKey(lambda a, b: a + b)
                             .collect())

                query_count_transit[qid][transit][partid] = tmp_count
                print "For", qid, transit, partid, "out is", tmp_count

    # print query_count_transit_fname

    for qid in qids:
        for ref_level_prev in refinement_levels:
            for ref_level_curr in refinement_levels:
                if ref_level_prev < ref_level_curr:
                    transit = (ref_level_prev, ref_level_curr)
                    if transit not in query_count_transit[qid]:
                        query_count_transit[qid][transit] = {}
                    for partid in partitioning_plans[qid]:
                        prev_qid = prev_qids[qid]
                        # print qid, ref_level_prev, ref_level_curr, partid
                        prev_level_out_rdd = load_rdd(
                            query_count_transit_fname[prev_qid][ref_level_prev][partitioning_plans[prev_qid][-1]], sc)
                        curr_level_rdd = load_rdd(
                            query_count_transit_fname[qid][ref_level_curr][partid], sc)
                        # print "prev level out", prev_level_out_rdd.take(2)
                        # print "curr level out", curr_level_rdd.take(2)
                        tmp_count_rdd = (curr_level_rdd.map(
                            lambda s: (str(IPNetwork(str(str(s[0][1]) + "/" + str(ref_level_prev))).network), s))
                                         .join(prev_level_out_rdd.map(lambda s: (s[0][1], 1)).distinct())
                                         .map(lambda s: s[1][0])
                                         )
                        # print tmp_count_rdd.take(2)
                        tmp_count = (tmp_count_rdd
                                     .map(lambda s: (s[0][0], 1))
                                     .reduceByKey(lambda a, b: a + b)
                                     .collect())
                        print "For", qid, transit, partid, "out is", tmp_count
                        query_count_transit[qid][transit][partid] = tmp_count

    print query_count_transit
    return query_count_transit


if __name__ == '__main__':
    fname = "/mnt/dirAB.out_00000_20160121080100.transformed.csv/part-00000"

    TD_PATH = '/mnt/caida_20160121080147_transformed'

    baseDir = os.path.join(TD_PATH)
    flows_File = os.path.join(baseDir, '*.csv')

    analyse_query(fname)
