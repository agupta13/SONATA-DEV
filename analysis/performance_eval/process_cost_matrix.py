import pickle
from sonata.core.lp.sonata_new_lp import solve_sonata_lp
from utils import *


def single_query_analysis():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"

    modes_2_out = {}

    sigma_max = 2
    width_max = 1
    bits_max = 2 * 1000000
    ref_levels = [0, 4, 8, 12, 16, 20, 24, 28, 32]
    # ref_levels = [0, 8, 16, 24, 32]
    modes = [2, 3, 4, 6]

    cost_matrix = prune_refinement_levels(fname, ref_levels)
    Q, query_2_tables, qid_2__r = get_lp_input(cost_matrix, ref_levels)

    modes = [2, 3, 6]
    qids = [2]

    for qid in qids:
        Q = [qid]
        print "*************"
        print "Query", qid
        for mode in modes:
            modes_2_out[mode] = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r, sigma_max, width_max,
                                                bits_max,
                                                mode)


def multi_query_analysis():
    fname = "data/aug_21_experiment_data_cost_matrix.pickle"

    modes_2_out = {}

    sigma_max = 12
    width_max = 2
    bits_max = 1.5 * 1000000
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
            print q_n, combo_id, Q
            for mode in modes:
                m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2__r, sigma_max, width_max, bits_max,
                                    mode)
                out[q_n][combo_id][mode] = m.objVal
    print out


if __name__ == '__main__':
    single_query_analysis()
    # multi_query_analysis()
    out = {1: {0: {2: 572040.0, 3: 77.0, 4: 479.0, 6: 77.0},
               1: {2: 47937.0, 3: 4.0, 4: 77.0, 6: 4.0},
               2: {2: 56695698.0, 3: 1318314.0, 4: 7323.0, 6: 2139.0},
               3: {2: 56695698.0, 3: 1235586.0, 4: 348.0, 6: 107.0},
               4: {2: 7972178.0, 3: 133.0, 4: 1537.0, 6: 133.0}},
           2: {0: {2: 619977.0, 3: 81.0, 4: 556.0, 6: 81.0},
               1: {2: 57267738.0, 3: 1318391.0, 4: 7802.0, 6: 2216.0},
               2: {2: 57267738.0, 3: 1235663.0, 4: 827.0, 6: 185.0},
               3: {2: 8544218.0, 3: 210.0, 4: 2016.0, 6: 210.0},
               4: {2: 56743635.0, 3: 1318318.0, 4: 7400.0, 6: 2143.0},
               5: {2: 56743635.0, 3: 1235590.0, 4: 425.0, 6: 112.0},
               6: {2: 8020115.0, 3: 137.0, 4: 1614.0, 6: 137.0},
               7: {2: 113391396.0, 3: 2553900.0, 4: 7671.0, 6: 2246.0},
               8: {2: 64667876.0, 3: 1318447.0, 4: 8860.0, 6: 2272.0},
               9: {2: 64667876.0, 3: 1235719.0, 4: 1885.0, 6: 240.0}},
           3: {0: {2: 57315675.0, 3: 1318395.0, 4: 7879.0, 6: 2220.0},
               1: {2: 57315675.0, 3: 1235667.0, 4: 904.0, 6: 188.0},
               2: {2: 8592155.0, 3: 214.0, 4: 2093.0, 6: 214.0},
               3: {2: 113963436.0, 3: 2553977.0, 4: 8150.0, 6: 2323.0},
               4: {2: 65239916.0, 3: 1318524.0, 4: 9339.0, 6: 2349.0},
               5: {2: 65239916.0, 3: 1235796.0, 4: 2364.0, 6: 318.0},
               6: {2: 113439333.0, 3: 2553904.0, 4: 7748.0, 6: 2250.0},
               7: {2: 64715813.0, 3: 1318451.0, 4: 8937.0, 6: 2276.0},
               8: {2: 121363574.0, 3: 2554033.0, 4: 9208.0, 6: 2380.0}},
           4: {0: {2: 114011373.0, 3: 2553981.0, 4: 314220.0, 6: 2327.0},
               1: {2: 65287853.0, 3: 1318528.0, 4: 12575.0, 6: 2353.0},
               2: {2: 65287853.0, 3: 1235800.0, 4: 5600.0, 6: 322.0},
               3: {2: 121935614.0, 3: 2554110.0, 4: 1290998.0, 6: 2470.0},
               4: {2: 121411511.0, 3: 2554037.0, 4: 149950.0, 6: 2383.0}},
           5: {0: {2: 121983551.0, 3: 2554114.0, 4: 1274557.0, 6: 2470.0}}
           }

    # rerun for [[3, 5, 6]] mode 6

