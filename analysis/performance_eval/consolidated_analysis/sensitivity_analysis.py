import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def vary_D():
    # fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"
    Ds = [1, 2, 4, 8, 12, 16, 32]
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 0.5 * bits_max_stage
    M = 2048
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)

    modes = [3, 4, 6]
    # modes = [6]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102],
                    12: [121, 122]}

    out = {}
    out[1] = {}
    N = 62874534

    Q = []
    for origin_qid in join_queries:
        Q += join_queries[origin_qid]

    print "*************"
    print "Query", Q
    for mode in modes:
        out[mode] = {}
        for D in Ds:
            out[mode][D] = []
            out[1][D] = []
            for minute in cost_matrix:
                cost_matrix_tmp = cost_matrix[minute]
                _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)
                print "$$", "mode", mode, "sigma", D
                m, _, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                    D, width_max, bits_max_stage, bits_max_register, mode,
                                    join_queries, M)
                out[mode][D].append(m.objVal)
                out[1][D].append(len(Q)*N)
                break
            # break

    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir + "sense_D_analysis.pickle"

    with open(out_fname, 'w') as f:
        print "Dumping data to file", out_fname, " ... "
        pickle.dump(out, f)

    print out


def vary_W():
    # fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    Ws = [1, 2, 4, 8, 12, 16, 32]
    sigma_max = 16
    bits_max_stage = 8 * 1000000
    bits_max_register = 0.5 * bits_max_stage
    M = 2048
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)

    modes = [2, 3, 4, 6]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102],
                    12: [121, 122]}

    out = {}
    out[1] = {}
    N = 62874534

    Q = []
    for origin_qid in join_queries:
        Q += join_queries[origin_qid]

    print "*************"
    print "Query", Q
    for mode in modes:
        out[mode] = {}
        for W in Ws:
            out[mode][W] = []
            out[1][W] = []
            for minute in cost_matrix:
                cost_matrix_tmp = cost_matrix[minute]
                _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)
                print "$$", "mode", mode, "W", W
                m, _, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                          sigma_max, W, bits_max_stage, bits_max_register, mode,
                                          join_queries, M)
                out[mode][W].append(m.objVal)
                out[1][W].append(len(Q)*N)
                break
                # break

    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir + "sense_W_analysis.pickle"

    with open(out_fname, 'w') as f:
        print "Dumping data to file", out_fname, " ... "
        pickle.dump(out, f)

    print out


def vary_B():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    Bs = [0.5, 1, 2, 4, 8, 12, 16, 32]
    sigma_max = 16
    width_max = 4
    M = 2048
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)

    modes = [2, 3, 4, 6]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102],
                    12: [121, 122]}

    out = {}
    out[1] = {}
    N = 62874534

    Q = []
    for origin_qid in join_queries:
        Q += join_queries[origin_qid]

    print "*************"
    print "Query", Q
    for mode in modes:
        out[mode] = {}
        for B in Bs:
            out[mode][B] = []
            out[1][B] = []
            bits_max_stage = B*1000000
            bits_max_register = bits_max_stage/2

            for minute in cost_matrix:
                cost_matrix_tmp = cost_matrix[minute]
                _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)
                print "$$", "mode", mode, "B", B
                m, _, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                          sigma_max, width_max, bits_max_stage, bits_max_register, mode,
                                          join_queries, M)
                out[mode][B].append(m.objVal)
                out[1][B].append(len(Q)*N)
                break

    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir + "sense_B_analysis.pickle"

    with open(out_fname, 'w') as f:
        print "Dumping data to file", out_fname, " ... "
        pickle.dump(out, f)

    print out


def vary_R():
    fname = "data/sept_5_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    sigma_max = 16
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 0.5 * bits_max_stage
    M = 2048

    modes = [4, 6]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102],
                    12: [121, 122]}

    # We need to fix queries 1, 4, & 11
    out = {}

    Q = []
    for origin_qid in join_queries:
        Q += join_queries[origin_qid]

    Rs = [[0, 4, 8, 12, 16, 20, 24, 28, 32], [0, 8, 16, 24, 32], [0, 16, 32], [0, 32]]

    for R in Rs:
        cost_matrix = prune_refinement_levels(fname, R)
        rBits = len(R)-1
        out[rBits] = {}
        print "***************"
        print Q, R
        for mode in modes:
            out[rBits][mode] = []
            for minute in cost_matrix:
                cost_matrix_tmp = cost_matrix[minute]
                _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, R)
                m, _, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                       sigma_max, width_max, bits_max_stage, bits_max_register,
                                       mode, join_queries, M)

                out[rBits][mode].append(m.objVal)
                break

    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir + "sense_R_analysis.pickle"

    with open(out_fname, 'w') as f:
        print "Dumping data to file", out_fname, " ... "
        pickle.dump(out, f)
    print out


def vary_M():
    fname = "data/sept_5_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    sigma_max = 16
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 0.5*bits_max_stage
    Ms = [128, 256, 512, 1024, 2048, 4096, 8192]
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)

    modes = [3, 4, 6]

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

    out = {}
    N = 62874534
    Q = []
    for origin_qid in join_queries:
        Q += join_queries[origin_qid]

    for M in Ms:
        out[M] = {}
        print "***************"
        print Q
        for mode in modes:
            out[M][mode] = []
            for minute in cost_matrix:
                cost_matrix_tmp = cost_matrix[minute]
                _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)
                m, _, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                       sigma_max, width_max, bits_max_stage, bits_max_register,
                                       mode, join_queries, M)
                if m.status != 3:
                    out[M][mode].append(m.objVal)
                else:
                    out[M][mode].append(len(Q)*N)

                break

    print out
    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir + "sense_M_analysis.pickle"

    with open(out_fname, 'w') as f:
        print "Dumping data to file", out_fname, " ... "
        pickle.dump(out, f)

if __name__ == '__main__':
    vary_D()
    vary_W()
    vary_B()
    vary_R()
    vary_M()