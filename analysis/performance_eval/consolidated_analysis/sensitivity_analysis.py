import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def vary_D():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    Ds = [1, 2, 4, 8, 12, 16, 32]
    width_max = 1
    bits_max = 1 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3, 4, 6]
    qids = [2, 5]

    out = {}

    Q = qids
    print "*************"
    print "Query", Q
    for mode in modes:
        out[mode] = {}
        for D in Ds:
            print "$$", "mode", mode, "sigma", D
            m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r, D, width_max, bits_max, mode)
            out[mode][D] = m.objVal

    # out_fname = "analysis/data/"+"sensitivity_sigma"+".pickle"
    #
    # with open(out_fname, 'w') as f:
    #     pickle.dump(out, f)

    print out


def vary_W():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    sigmas = [1, 2, 4, 8, 12, 16, 32]
    sigma_max = 2
    width_max = 1
    bits_max = 2 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    modes = [2, 3, 4, 6]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3]
    qids = [2, 5, 6, 7]

    out = {}


    Q = [qids]
    print "*************"
    print "Queries", Q
    for mode in modes:
        out[mode] = {}
        for sigma in sigmas:
            print "$$", "mode", mode, "sigma", sigma
            m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r, sigma, width_max, bits_max, mode)
            out[mode][sigma] = m.objVal

    # out_fname = "analysis/data/"+"sensitivity_width"+".pickle"
    #
    # with open(out_fname, 'w') as f:
    #     pickle.dump(out, f)

    print out

# def vary_memory():
#

if __name__ == '__main__':
    vary_D()
    # vary_width()
    # vary_memory()