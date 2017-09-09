#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import pickle
import os
import glob

queries_2_operator = {
    1: {2: 'Reduce', 3: 'Filter'},
    2: {2: 'Reduce', 3: 'Filter'},
    3: {2: 'Distinct', 4: 'Reduce', 5: 'Filter'},
    4: {3: 'Reduce', 4: 'Filter'},
    5: {2: 'Distinct', 4: 'Reduce', 5: 'Filter'},
    6: {2: 'Distinct', 4: 'Reduce', 5: 'Filter'},
    7: {2: 'Distinct', 4: 'Reduce', 5: 'Filter'},
    91: {2: 'Reduce'}, 92: {2: 'Reduce'}, 93: {2: 'Reduce'},
    101: {2: 'Reduce'}, 102: {2: 'Reduce'},
    111: {2: 'Reduce', 3: 'Filter'}, 112: {2: 'Reduce'},
    121: {2: 'Distinct', 4: 'Reduce', 5: 'Filter'}, 122: {2: 'Reduce'}
}


def generate_cost_matrix_from_counts():
    counts_path = "data/all_data/*.pickle"
    cost_matrix = {}
    for fname in glob.iglob(counts_path):
        qid = int(fname.split("transit_")[1].split("_")[0])
        cost_matrix[qid] = {}
        with open(fname) as f:
            counts = pickle.load(f)
            for transit in counts[qid]:
                cost_matrix[qid][transit] = {}
                tids = counts[qid][transit].keys()
                tids.sort()
                for (tid1, tid2) in zip(tids[:-1], tids[1:]):
                    n_packets_in = counts[qid][transit][tid1][0][1]
                    n_packets_out = 0
                    n_bits = 0
                    if queries_2_operator[qid][tid2 % 1000] in {'Reduce', 'Distinct'}:
                        for tid3 in tids[1:]:
                            if queries_2_operator[qid][tid3 % 1000] == 'Reduce':
                                n_bits += 32 * counts[qid][transit][tid3][0][1]
                                n_packets_out = counts[qid][transit][tid2 + 1][0][1]

                            elif queries_2_operator[qid][tid3 % 1000] == 'Distinct':
                                n_bits += 1 * counts[qid][transit][tid3][0][1]
                                n_packets_out = counts[qid][transit][tid2][0][1]

                            if tid3 == tid2:
                                break
                        print qid, transit, tid2 % 1000, (n_packets_in, n_bits, n_packets_out)
                        cost_matrix[qid][transit][tid2 % 1000] = (n_packets_in, n_bits, n_packets_out)

    # out_fname = "data/test.pickle"
    # with open(out_fname, 'w') as f:
    #     print "Dumping cost matrix to file", out_fname, " ... "
    #     pickle.dump(cost_matrix, f)

    print cost_matrix

    return cost_matrix


qid_2_qids = {
    9 : [91, 92, 93],
    10: [101, 102],
    11: [111, 112],
    12: [121, 122]
}


def generate_cost_matrix_from_counts_for_join():
    counts_path = "data/all_data/*.pickle"
    cost_matrix = {}
    for fname in glob.iglob(counts_path):
        qid_origin = int(fname.split("transit_")[1].split("_")[0])
        print qid_origin
        if qid_origin in qid_2_qids:
            with open(fname) as f:
                counts = pickle.load(f)
                qids = qid_2_qids[qid_origin]
                for qid in qids:
                    cost_matrix[qid] = {}
                    for transit in counts[qid]:
                        print qid, transit, counts[qid][transit]
                        cost_matrix[qid][transit] = {}
                        tids = counts[qid][transit].keys()
                        tids.sort()
                        for (tid1, tid2) in zip(tids[:-1], tids[1:]):
                            if len(counts[qid][transit][tid1]) == 0:
                                # case where cost_matrix[qid][transit] is like {0: [], 2: []}
                                for tid in tids[1:]:
                                    cost_matrix[qid][transit][tid % 1000] = (0, 0, 0)

                            else:
                                n_packets_in = counts[qid][transit][tid1][0][1]
                                n_packets_out = 0
                                n_bits = 0
                                if queries_2_operator[qid][tid2 % 1000] in {'Reduce', 'Distinct'}:
                                    for tid3 in tids[1:]:
                                        if queries_2_operator[qid][tid3 % 1000] == 'Reduce':
                                            n_bits += 32 * counts[qid][transit][tid3][0][1]
                                            if tid2 + 1 in queries_2_operator[qid]:
                                                n_packets_out = counts[qid][transit][tid2 + 1][0][1]
                                            else:
                                                n_packets_out = counts[qid][transit][tid2][0][1]

                                        elif queries_2_operator[qid][tid3 % 1000] == 'Distinct':
                                            n_bits += 1 * counts[qid][transit][tid3][0][1]
                                            n_packets_out = counts[qid][transit][tid2][0][1]

                                        if tid3 == tid2:
                                            break
                                    print qid, transit, tid2 % 1000, (n_packets_in, n_bits, n_packets_out)
                                    cost_matrix[qid][transit][tid2 % 1000] = (n_packets_in, n_bits, n_packets_out)
    print cost_matrix


if __name__ == '__main__':
    # cost_matrix = generate_cost_matrix_from_counts()
    generate_cost_matrix_from_counts_for_join()
