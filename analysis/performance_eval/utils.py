import pickle


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