import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp


def prune_refinement_levels(fname, ref_levels):
    out_cost_matrix = {}
    with open(fname, 'r') as f:
        in_cost_matrix = pickle.load(f)
        for qid in in_cost_matrix:
            out_cost_matrix[qid] = {}
            for (r1, r2) in in_cost_matrix[qid]:
                if r1 in ref_levels and r2 in ref_levels:
                    out_cost_matrix[qid][(r1, r2)] = in_cost_matrix[qid][(r1, r2)]

    return out_cost_matrix


def get_lp_input(cost_matrix, ref_levels):
    Q = []
    query_2_tables = {}
    qid_2__r = {}

    for qid in cost_matrix:
        Q.append(qid)
        qid_2__r[qid] = ref_levels
        query_2_tables[qid] = cost_matrix[qid][(0, 32)].keys()
        query_2_tables[qid].sort()

    return Q, query_2_tables, qid_2__r


if __name__ == '__main__':
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"

    modes_2_out = {}

    sigma_max = 6
    width_max = 4
    bits_max = 8000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    ref_levels = [0, 8, 16, 24, 32]
    modes = [2, 3, 4, 6]


    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3, 6]
    Q = [6]

    for mode in modes:
        modes_2_out[mode] = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r, sigma_max, width_max, bits_max,
                                            mode)
