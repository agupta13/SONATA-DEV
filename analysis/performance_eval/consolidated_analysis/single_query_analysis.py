import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def single_query_analysis():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    fname = "data/sept_5_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    sigma_max = 12
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 0.5 * bits_max_stage
    M = 2048
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    modes = [2, 3, 4, 6]
    # modes = [4, 6]
    join_queries = {
                    2: [2],
                    3: [3],
                    5: [5],
                    6: [6],
                    7: [7],
                    9: [91, 92, 93],
                    10: [101, 102],
                    12: [121, 122]
                    }

    modes_2_out = {}
    cost_matrix = prune_refinement_levels(fname, ref_levels)
    out = {}
    N_in = 62874534
    # M = 512
    for qid in join_queries.keys()[:]:
        if True:
            out[qid] = {}
            Q = [qid]
            print "*************"
            print "Query", qid
            for mode in modes:
                out[qid][mode] = []
                out[qid][1] = []
                for minute in cost_matrix:
                    cost_matrix_tmp = cost_matrix[minute]
                    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)

                    m, _, _ = solve_sonata_lp(join_queries[qid], query_2_tables, cost_matrix_tmp, qid_2__r,
                                           sigma_max, width_max, bits_max_stage, bits_max_register, mode,
                                           join_queries, M)
                    modes_2_out[mode] = m
                    out[qid][mode].append(int(m.objVal))
                    out[qid][1].append(len(join_queries[qid]) * N_in)
                    break

    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir + "single_query_analysis.pickle"

    with open(out_fname, 'w') as f:
        print "Dumping data to file", out_fname, " ... "
        pickle.dump(cost_matrix, f)

    print out


if __name__ == '__main__':
    single_query_analysis()
