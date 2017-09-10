import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *


def ref_level_analysis():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"

    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)
    print query_2_tables

    qids = range(1, 8)
    ref_level_data = {}

    for qid in qids:
        ref_level_data[qid] = {}
        print "*************"
        print "Query", qid
        tid_min = min(cost_matrix[qid][(0, 8)].keys())
        base = cost_matrix[qid][(0, 8)][tid_min][0]
        for ref_prev, r_curr in zip(ref_levels[1:-1], ref_levels[2:]):
            transit = (ref_prev, r_curr)
            out = float(cost_matrix[qid][transit][tid_min][0]) / base
            print qid, ref_prev, cost_matrix[qid][transit][tid_min][0], out
            ref_level_data[qid][ref_prev] = out

    print ref_level_data
    out_dir = "analysis/performance_eval/plot_results/plot_data/"
    out_fname = out_dir+"ref_level_analysis.pickle"

    with open(out_fname, 'w') as f:
        print "Dumping data to file", out_fname, " ... "
        pickle.dump(cost_matrix, f)


if __name__ == '__main__':
    ref_level_analysis()
