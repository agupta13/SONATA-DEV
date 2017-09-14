#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import time
import datetime
import math
import json
import glob
import os

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
                        .filter(lambda s: s[0] != 'ts')
                        .map(lambda s: tuple([str(s[0]).split(".")[0]]+list(s[1:7]) + list(s[9:11]) + get_tcp_flag(s[11])))
                        )
    # print transformed_data.take(5)

    """
    ts,sIP,sPort,dIP,dPort,nBytes,proto,tcp.seq,tcp.ack,tcp.flags
    """

    lines = transformed_data.map(toCSVLine)
    lines.saveAsTextFile(out_fname)


def transform_input_data():
    base_dir = "/mnt/data/"
    minutes = ["1301", "1302", "1303", "1304", "1305", "1306"]
    # minutes = ["1301"]
    sc = create_spark_context()
    for minute in minutes:
        in_dir_name = base_dir+minute+"/"
        out_dir_name = base_dir+minute+"_transformed/"
        fnames = os.listdir(in_dir_name)
        for fname in fnames:
            if ".csv" in fname:
                full_fname_in = in_dir_name+fname
                full_fname_out = out_dir_name+fname
                print "Transforming file", full_fname_in, "to", full_fname_out
                reduce_size(sc, full_fname_in, full_fname_out)


if __name__ == '__main__':
    transform_input_data()


