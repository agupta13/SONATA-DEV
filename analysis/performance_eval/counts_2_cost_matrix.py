#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)
#  This file is written because someone
#  who goes by the name of Arpit Gupta forgot to dump the cost pickles---how careless :(.
#  As it will cost another 13 hours to generate the costs data, we have to spend time on this redundant dev effort.

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
    7: {2: 'Distinct', 4: 'Reduce', 5: 'Filter'}
}


def generate_cost_matrix_from_counts():
    counts_path = "data/aug_21_experiment_data/*.pickle"
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
                    if queries_2_operator[qid][tid2%1000] in {'Reduce', 'Distinct'}:
                        for tid3 in tids[1:]:
                            if queries_2_operator[qid][tid3%1000] == 'Reduce':
                                n_bits += 32*counts[qid][transit][tid3][0][1]
                                n_packets_out = counts[qid][transit][tid2+1][0][1]

                            elif queries_2_operator[qid][tid3%1000] == 'Distinct':
                                n_bits += 1*counts[qid][transit][tid3][0][1]
                                n_packets_out = counts[qid][transit][tid2][0][1]

                            if tid3 == tid2:
                                break
                        print qid, transit, tid2%1000, (n_packets_in, n_bits, n_packets_out)
                        cost_matrix[qid][transit][tid2%1000] = (n_packets_in, n_bits, n_packets_out)

    out_fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    with open(out_fname, 'w') as f:
        print "Dumping cost matrix to file", out_fname, " ... "
        pickle.dump(cost_matrix, f)

    return cost_matrix


if __name__ == '__main__':
    cost_matrix = generate_cost_matrix_from_counts()

