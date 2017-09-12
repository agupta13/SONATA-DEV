import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def single_query_analysis():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    fname = "data/sept_5_experiment_data_cost_matrix.pickle"

    modes_2_out = {}

    sigma_max = 12
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 4 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    cost_matrix = prune_refinement_levels(fname, ref_levels)

    for transit in cost_matrix[111]:
        print transit, cost_matrix[111][transit]

    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [3, 6]
    qids = range(1, 8)
    qids = [2]
    Q = [2, 3, 5, 6, 7, 9, 10, 12]
    join_queries = {2: [2],
                    3: [3],
                    5: [5],
                    6: [6],
                    7: [7],
                    9: [91, 92, 93],
                    10: [101, 102],
                    # 11: [111, 112],
                    12: [121, 122]
                    }

    out = {}
    N_in = 56695698
    for qid in join_queries:
        out[qid] = {}
        Q = [qid]
        print "*************"
        print "Query", qid
        out[qid][1] = N_in * len(join_queries[qid])
        for mode in modes:
            m = solve_sonata_lp(join_queries[qid], query_2_tables, cost_matrix, qid_2__r,
                                sigma_max, width_max, bits_max_stage,
                                bits_max_register, mode, join_queries)
            modes_2_out[mode] = m
            out[qid][mode] = m.objVal

    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir + "single_query_analysis.pickle"

    # with open(out_fname, 'w') as f:
    #     print "Dumping data to file", out_fname, " ... "
    #     pickle.dump(cost_matrix, f)

    print out


if __name__ == '__main__':
    single_query_analysis()
