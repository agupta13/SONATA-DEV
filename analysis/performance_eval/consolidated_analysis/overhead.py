from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from analysis.performance_eval.utils import *
import numpy as np


def overhead_analysis():
    # fname = "data/aug_21_experiment_data_cost_matrix.pickle"
    fname = "data/sept_5_experiment_data_cost_matrix.pickle"
    fname = "data/sept_16_experiment_data_cost_matrix.pickle"

    sigma_max = 16
    width_max = 4
    bits_max_stage = 8 * 1000000
    bits_max_register = 0.5*bits_max_stage
    M = 2048
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]

    with open(fname, 'r') as f:
        counts = pickle.load(f)
        minutes = counts.keys()
        minutes.sort()
        print minutes

    cost_matrix = prune_refinement_levels(fname, ref_levels)

    modes = [6]

    join_queries = {2: [2], 3: [3], 5: [5], 6: [6], 7: [7], 9: [91, 92, 93], 10: [101, 102], 11: [111, 112],
                    12: [121, 122]}
    out_qid_for_join_queries = {91: 94, 92: 94, 93: 94, 101: 103,
                        102: 103, 111: 113, 112: 113, 121: 123, 122: 123}

    # We need to fix queries 1, 4, & 11
    all_queries = {
        # 1: [[5]],
        # 2: [[5, 9]],
        # 3: [[5, 9, 7]],
        # 4: [[5, 9, 7, 12]],
        # 5: [[5, 9, 7, 12, 6]],
        # 6: [[5, 9, 7, 12, 6, 2]],
        # 7: [[5, 9, 7, 12, 6, 2, 10]],
        8: [[5, 9, 7, 12, 6, 2, 10, 3]]
    }
    out = {}
    overhead = {}
    register_updates = {}

    for q_n in all_queries.keys()[:]:
        overhead[q_n] = {}
        register_updates[q_n] = {}
        for combo_id in range(len(all_queries[q_n])):
            overhead[q_n][combo_id] = {}
            register_updates[q_n][combo_id] = {}
            Q = list()
            for origin_qid in all_queries[q_n][0]:
                print join_queries[origin_qid]
                Q += join_queries[origin_qid]
            print "***************"
            print q_n, combo_id, Q
            for mode in modes:
                overhead[q_n][combo_id][mode] = []
                register_updates[q_n][combo_id][mode] = []
                for minute in minutes:
                    cost_matrix_tmp = cost_matrix[minute]
                    _, query_2_tables, qid_2__r = get_lp_input(cost_matrix_tmp, ref_levels)
                    m, refinement_levels, Last = solve_sonata_lp(Q, query_2_tables, cost_matrix_tmp, qid_2__r,
                                                           sigma_max, width_max, bits_max_stage, bits_max_register,
                                                           mode,
                                                           join_queries, M)
                    tmp = 0
                    reads = 0
                    for qid in Last:
                        levels = [int(x) for x in refinement_levels[qid].split('-->')]
                        for (rid1, rid2) in  zip(levels[:-1], levels[1:]):
                            for tid in Last[qid][rid2]:
                                if Last[qid][rid2][tid].x > 0.5:
                                    if qid not in out_qid_for_join_queries:
                                        if tid > 2:
                                            print qid, rid2, tid, cost_matrix_tmp[qid][(rid1, rid2)][tid]
                                            reads += cost_matrix_tmp[qid][(rid1, rid2)][tid][-1]


                    for qid in Q:
                        levels = [int(x) for x in refinement_levels[qid].split('-->')]
                        for transit in zip(levels[:-1], levels[1:]):
                            if qid in out_qid_for_join_queries:
                                # out_qid = out_qid_for_join_queries[qid]
                                # # print counts[minute][out_qid][transit]
                                # final_key = max(counts[minute][out_qid][transit].keys())
                                # updates = counts[minute][out_qid][transit][final_key][-1]
                                # not counting for the join queries for now
                                updates = 0
                            else:
                                final_key = max(cost_matrix_tmp[qid][transit].keys())
                                updates = cost_matrix_tmp[qid][transit][final_key][-1]
                            if 32 not in transit:
                                tmp += updates
                                print qid, transit, final_key, updates
                    print q_n, mode, minute, tmp
                    overhead[q_n][combo_id][mode].append(tmp)
                    register_updates[q_n][combo_id][mode].append(reads)
                    break
                # break
            # break

    print overhead
    print register_updates


# with open("data/multi_minute_data/query_counts_transit_7_2017-09-14-17:52:03.517540_1301.pickle", 'r') as f:
#     data = pickle.load(f)
#     print data[7][(0,32)]

overhead_analysis()
