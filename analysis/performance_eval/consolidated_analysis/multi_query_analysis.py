import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def multi_query_analysis():
    # fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    # fname = "data/sept_5_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    sigma_max = 12
    width_max = 2
    bits_max_stage = 8 * 1000000
    bits_max_register = 4 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    M = 2048
    modes = [2, 3, 4, 6]
    # modes = [6]
    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102], 11: [111, 112],
                    12: [121, 122]}
    # We need to fix queries 1, 4, & 11
    all_queries = {
        1: [[5]],
        2: [[5, 9]],
        3: [[5, 9, 7]],
        4: [[5, 9, 7, 12]],
        5: [[5, 9, 7, 12, 6]],
        6: [[5, 9, 7, 12, 6, 2]],
        7: [[5, 9, 7, 12, 6, 2, 10]],
        8: [[5, 9, 7, 12, 6, 2, 10, 3]]
    }
    out = {}
    N_in = 62874534

    cost_matrix = prune_refinement_levels(fname, ref_levels)

    for q_n in all_queries:
        out[q_n] = {}
        for combo_id in range(len(all_queries[q_n])):
            out[q_n][combo_id] = {}
            Q = list()
            for origin_qid in all_queries[q_n][0]:
                print join_queries[origin_qid]
                Q += join_queries[origin_qid]
            print "***************"
            print q_n, combo_id, Q
            for mode in modes:
                out[q_n][combo_id][mode] = []
                out[q_n][combo_id][1] = []
                for minute in cost_matrix:
                    cost_matrix_tmp = cost_matrix[minute]
                    _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)

                    m, _, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                              sigma_max, width_max, bits_max_stage, bits_max_register, mode,
                                              join_queries, M)

                    out[q_n][combo_id][mode].append(int(m.objVal))

                    out[q_n][combo_id][1].append(sum([len(join_queries[qid]) for qid in all_queries[q_n][combo_id]]) * N_in)
                    break

    print out


if __name__ == '__main__':
    multi_query_analysis()
