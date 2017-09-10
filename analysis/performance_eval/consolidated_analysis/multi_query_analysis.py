import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def multi_query_analysis():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"

    modes_2_out = {}

    sigma_max = 4
    width_max = 1
    bits_max = 1 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    # ref_levels = [0, 8, 16, 24, 32]
    modes = [2, 3, 4, 6]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3, 4, 6]
    modes = [6]
    Q = [2, 3, 5, 6, 7]
    all_queries = {
        # 1: [[2], [3], [5], [6], [7]],
        # 2: [[2, 3], [2, 5], [2, 6], [2, 7], [3, 5], [3, 6], [3, 7], [5, 6], [5, 7], [6, 7]],
        # 3: [[2, 3, 5], [2, 3, 6], [2, 3, 7], [2, 5, 6], [2, 5, 7], [2, 6, 7], [3, 5, 6], [3, 5, 7],
        #     [5, 6, 7]],
        # 4: [[2, 3, 5, 6], [2, 3, 5, 7], [2, 3, 6, 7], [2, 5, 6, 7], [3, 5, 6, 7]],
        5: [[2, 3, 5, 6, 7]]
    }
    out = {}

    for q_n in all_queries:
        out[q_n] = {}
        for combo_id in range(len(all_queries[q_n])):
            out[q_n][combo_id] = {}
            Q = all_queries[q_n][combo_id]
            Q = [2,3]
            print q_n, combo_id, Q
            for mode in modes:
                m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r, sigma_max, width_max, bits_max,
                                    mode, {0:[2,3]})
                out[q_n][combo_id][mode] = m.objVal
    print out

if __name__ == '__main__':
    multi_query_analysis()