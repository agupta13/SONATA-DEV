import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *
import numpy as np


def get_delta_distribution():
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"
    join_queries = {1: [1], 2: [2], 3: [3], 4: [4], 5: [5], 6: [6], 7: [7]}
    with open(fname, 'r') as f:
        counts = pickle.load(f)
        minutes = counts.keys()
        minutes.sort()
        print minutes
        # print counts[1301][122][(0,32)], counts[1302][122][(0,32)]
        delta = {}
        rprev_2_delta = {}
        cdf_out = []
        for origin_qid in join_queries:
            for qid in join_queries[origin_qid]:
                delta[qid] = {}
                rprev_2_delta[qid] = {}
                for transit in counts[minutes[0]][qid]:
                    r_prev = transit[0]
                    if r_prev not in rprev_2_delta[qid]:
                        rprev_2_delta[qid][r_prev] = []
                    delta[qid][transit] = {}
                    for tid in counts[minutes[0]][qid][transit]:
                        tmp = [counts[x][qid][transit][tid][1] for x in minutes[:4]]
                        tmp = [x for x in tmp if x > 0]
                        tmp_delta = (100.0 * (max(tmp) - min(tmp))) / min(tmp)
                        tmp_delta = np.std([float(x) / max(tmp) for x in tmp])
                        print qid, transit, tid, tmp, tmp_delta
                        delta[qid][transit][tid] = tmp_delta
                        rprev_2_delta[qid][r_prev].append(tmp_delta)
                        cdf_out.append(tmp_delta)
                print max(rprev_2_delta[qid])
        print cdf_out
        print len(cdf_out), max(cdf_out), min(cdf_out)


def inflate_cost_matrix(in_cost_matrix, deltaX):
    out_cost_matrix = {}
    for qid in in_cost_matrix:
        out_cost_matrix[qid] = {}
        for transit in in_cost_matrix[qid]:
            out_cost_matrix[qid][transit] = {}
            for tid in in_cost_matrix[qid][transit]:
                n1, b, n2 = in_cost_matrix[qid][transit][tid]
                b_new = int(b * (1 + float(deltaX) / 100))
                # inflate the number of bits required
                out_cost_matrix[qid][transit][tid] = (n1, b_new, n2)
                # if b > 0:
                #     print b, b_new, float(b_new-b)/b

    return out_cost_matrix


def vary_DeltaB():
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"
    sigma_max = 12
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 2 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)


    modes = [6]
    origin_qids = [2, 5]

    join_queries = {1: [1], 2: [2], 3: [3], 4: [4], 5: [5], 6: [6], 7: [7]}

    out = {}
    N = 56695698

    Q = []
    for origin_qid in origin_qids:
        Q += join_queries[origin_qid]

    deltaXs = [0, 20, 40, 60, 80, 100]

    print "*************"
    print "Query", Q
    for minute in cost_matrix:
        out[minute] = {}
        # out[minute][1] = {}
        for mode in modes:
            out[minute][mode] = {}
            for deltaX in deltaXs:
                print "$$", "mode", mode, "deltaX", deltaX
                cost_matrix_tmp = inflate_cost_matrix(cost_matrix[minute], deltaX)
                # cost_matrix_tmp = cost_matrix[minute]
                Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)
                origin_qids = [2, 3, 5, 6, 7]
                Q = []
                for origin_qid in origin_qids:
                    Q += join_queries[origin_qid]

                m, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                    sigma_max, width_max, bits_max_stage, bits_max_register, mode,
                                    join_queries)
                out[minute][mode][deltaX] = m.objVal
                # hardcode mode 1 values. I know this is inefficient, but it is correct.
                # out[minute][1][deltaX] = len(Q) * N
        break

    # out_fname = "analysis/data/"+"sensitivity_sigma"+".pickle"
    #
    # with open(out_fname, 'w') as f:
    #     pickle.dump(out, f)

    print out


# get_delta_distribution()
vary_DeltaB()
