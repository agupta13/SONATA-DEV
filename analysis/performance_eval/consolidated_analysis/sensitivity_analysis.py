import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def vary_D():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    Ds = [1, 2, 4, 8, 12, 16, 32]
    width_max = 2
    bits_max_stage = 8 * 1000000
    bits_max_register = 4 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3, 4, 6]
    origin_qids = [2, 5]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102], 11: [111, 112],
                    12: [121, 122]}

    out = {}
    out[1] = {}
    N = 56695698

    Q = []
    for origin_qid in origin_qids:
        Q += join_queries[origin_qid]

    print "*************"
    print "Query", Q
    for mode in modes:
        out[mode] = {}
        for D in Ds:
            print "$$", "mode", mode, "sigma", D
            m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r,
                                D, width_max, bits_max_stage, bits_max_register, mode,
                                join_queries)
            out[mode][D] = m.objVal
            # hardcode mode 1 values. I know this is inefficient, but it is correct.
            out[1][D] = len(Q)*N

    # out_fname = "analysis/data/"+"sensitivity_sigma"+".pickle"
    #
    # with open(out_fname, 'w') as f:
    #     pickle.dump(out, f)

    print out


def vary_W():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    Ws = [1, 2, 4, 8, 12, 16, 32]
    sigma_max = 12
    bits_max_stage = 8 * 1000000
    bits_max_register = 4 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3, 4, 6]
    origin_qids = [2, 5]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102], 11: [111, 112],
                    12: [121, 122]}

    out = {}
    out[1] = {}
    N = 56695698

    Q = []
    for origin_qid in origin_qids:
        Q += join_queries[origin_qid]

    print "*************"
    print "Query", Q
    for mode in modes:
        out[mode] = {}
        for W in Ws:
            print "$$", "mode", mode, "Width", W
            m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r,
                                sigma_max, W, bits_max_stage, bits_max_register, mode,
                                join_queries)
            out[mode][W] = m.objVal
            # hardcode mode 1 values. I know this is inefficient, but it is correct.
            out[1][W] = len(Q)*N

    # out_fname = "analysis/data/"+"sensitivity_sigma"+".pickle"
    #
    # with open(out_fname, 'w') as f:
    #     pickle.dump(out, f)

    print out


def vary_B():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    Bs = [0.5, 1, 2, 4, 8, 12, 16, 32]
    sigma_max = 12
    width_max = 2
    bits_max_stage = 8 * 1000000
    bits_max_register = 4 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3, 4, 6]
    origin_qids = [2, 5]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102], 11: [111, 112],
                    12: [121, 122]}

    out = {}
    out[1] = {}
    N = 56695698

    Q = []
    for origin_qid in origin_qids:
        Q += join_queries[origin_qid]

    print "*************"
    print "Query", Q
    for mode in modes:
        out[mode] = {}
        for B in Bs:
            bits_max_stage = B*1000000
            bits_max_register = bits_max_stage/2

            print "$$", "mode", mode, "bits stage", bits_max_stage, "bits register", bits_max_register
            m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r,
                                sigma_max, width_max, bits_max_stage, bits_max_register, mode,
                                join_queries)
            out[mode][B] = m.objVal
            # hardcode mode 1 values. I know this is inefficient, but it is correct.
            out[1][B] = len(Q)*N

    # out_fname = "analysis/data/"+"sensitivity_sigma"+".pickle"
    #
    # with open(out_fname, 'w') as f:
    #     pickle.dump(out, f)

    print out


def vary_R():
    fname = "data/sept_5_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    sigma_max = 12
    width_max = 2
    bits_max_stage = 8 * 1000000
    bits_max_register = 4 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    with open(fname, 'r') as f:
        counts = pickle.load(f)
        minutes = counts.keys()
        minutes.sort()
        print minutes



    modes = [6]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102], 11: [111, 112],
                    12: [121, 122]}

    # We need to fix queries 1, 4, & 11
    Q = []
    origin_qids = [5, 9, 7, 12, 6, 2, 10, 3]
    for origin_qid in origin_qids:
        Q += join_queries[origin_qid]
    out = {}
    Rs = [[0, 4, 8, 12, 16, 20, 24, 28, 32], [0, 8, 16, 24, 32], [0, 16, 32], [0, 32]]

    for R in Rs:
        cost_matrix = prune_refinement_levels(fname, R)
        rBits = len(R)-1
        out[rBits] = {}
        print "***************"
        print Q
        for mode in modes:
            out[rBits][mode] = []
            for minute in minutes:
                cost_matrix_tmp = cost_matrix[minute]
                _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, R)
                m, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                       sigma_max, width_max, bits_max_stage, bits_max_register,
                                       mode, join_queries)
                tmp = m.objVal

                out[rBits][mode].append(tmp)
                break
                # break
                # break

    print out


def vary_M():
    fname = "data/sept_5_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    sigma_max = 12
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 0.5*bits_max_stage
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    with open(fname, 'r') as f:
        counts = pickle.load(f)
        minutes = counts.keys()
        minutes.sort()
        print minutes

    modes = [6]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102], 11: [111, 112],
                    12: [121, 122]}

    # We need to fix queries 1, 4, & 11
    Q = []
    origin_qids = [5, 9, 7, 12, 6, 2, 10, 3]
    for origin_qid in origin_qids:
        Q += join_queries[origin_qid]

    Q = [5]
    out = {}
    R = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    Ms = [128, 256, 512, 1024, 2048, 4096]
    # Ms = [256]
    for M in Ms:
        cost_matrix = prune_refinement_levels(fname, R)
        out[M] = {}
        print "***************"
        print Q
        for mode in modes:
            out[M][mode] = []
            for minute in minutes:
                cost_matrix_tmp = cost_matrix[minute]
                _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, R)
                m, _, _ = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                       sigma_max, width_max, bits_max_stage, bits_max_register,
                                       mode, join_queries, M)
                tmp = m.objVal

                out[M][mode].append(tmp)
                break
                # break
                # break

    print out

if __name__ == '__main__':
    # vary_D()
    # vary_W()
    # vary_B()
    # vary_R()
    vary_M()