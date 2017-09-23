from __future__ import print_function

from gurobipy import Model, GRB, GurobiError
from tabulate import tabulate
import math
import itertools


def solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max_stage,
                    bits_max_register, mode=6, join_queries={}, M_max = 4096):
    """
    :param Q:
    :param query_2_tables:
    :param cost_matrix:
    :param qid_2_R:
    :param sigma_max:
    :param width_max:
    :param bits_max_stage:
    :param mode:
    :param join_queries:
    :return:

    Mode:
    1: All SP, i.e. no operation in the data plane
    2: FILTER-ONLY, i.e. only perform filter operations in the data plane
    3: PART-ONLY, i.e. naively execute stateful operations in the data plane w/o any refinement
    4: FIXED-REF, i.e. in addition to naive partitioning, also refine the input query with a one
        refinement plan fits all refinement plan
    5: Cache:, i.e. N-way LRU cache // we don't solve any LP for this mode. It is here only for the sake of completeness
    6: Sonata
    """
    name = "sonata"
    # Create a new model
    m = Model(name)
    I = {}
    D = {}
    Last = {}
    S = {}
    F = {}
    query_2_n = {}
    Sigma = {}
    M = {}
    ML = {}

    var_name = "sigma"
    dp_sigma = m.addVar(lb=0, ub=sigma_max, vtype=GRB.INTEGER, name=var_name)
    m.addConstr(dp_sigma <= sigma_max)

    for qid in Q:
        I[qid] = {}
        D[qid] = {}
        Last[qid] = {}
        S[qid] = {}
        F[qid] = {}
        Sigma[qid] = {}
        query_2_n[qid] = {}
        M[qid] = {}
        ML[qid] = {}
        for rid in qid_2_R[qid][1:]:
            # create indicator variable for refinement level rid for query qid
            var_name = "i_" + str(qid) + "_" + str(rid)
            I[qid][rid] = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)

            # create indicator variable for metadata for refinement level rid for query qid
            # Metadata (index, refinement key, report fields) will be only added to the packet
            # when any stateful operator is executed in the data plane
            # qid is added by default for all queries
            var_name = "M_" + str(qid) + "_" + str(rid)
            M[qid][rid] = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)
            # m.addConstr(M[qid][rid] >= I[qid][rid])

            var_name = "ML_" + str(qid) + "_" + str(rid)
            ML[qid][rid] = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)

            if rid == qid_2_R[qid][1:][-1]:
                m.addConstr(I[qid][rid] == 1)

            D[qid][rid] = {}
            Last[qid][rid] = {}
            S[qid][rid] = {}
            F[qid][rid] = {}
            Sigma[qid][rid] = {}

            table_2_n = {}

            # add f variables for each previous refinement level
            for rid_prev in qid_2_R[qid]:
                # add f variable for previous refinement level rid_prev for table tid_new
                if rid_prev < rid:
                    var_name = "f_" + str(qid) + "_" + str(rid_prev) + "_" + str(rid)
                    F[qid][rid][rid_prev] = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)
                    if rid_prev > 0:
                        m.addConstr(F[qid][rid][rid_prev] <= I[qid][rid_prev])

            # sum of all f variables for each table is at max 1
            f_over__qr = [F[qid][rid][rid_prev] for rid_prev in F[qid][rid].keys()]
            m.addConstr(sum(f_over__qr) >= I[qid][rid])
            m.addConstr(sum(f_over__qr) <= 1)

            for tid in query_2_tables[qid]:
                tid_new = 1000000 * qid + 1000 * tid + rid
                # add stage variable
                var_name = "sigma_" + str(tid_new)
                Sigma[qid][rid][tid] = m.addVar(lb=0, ub=sigma_max, vtype=GRB.INTEGER, name=var_name)
                m.addConstr(dp_sigma >= Sigma[qid][rid][tid])

                # add d variable for this table
                var_name = "d_" + str(tid_new)
                D[qid][rid][tid] = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)

                S[qid][rid][tid] = {}
                for sid in range(1, sigma_max + 1):
                    # add f variable for stage sid rid_prev for table tid_new
                    var_name = "s_" + str(tid_new) + "_" + str(sid)
                    S[qid][rid][tid][sid] = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)
                    m.addGenConstrIndicator(S[qid][rid][tid][sid], True, Sigma[qid][rid][tid] == sid)

                # a table can at most use one stage
                s_over_t = [S[qid][rid][tid][sid] for sid in range(1, sigma_max + 1)]
                m.addConstr(sum(s_over_t) >= D[qid][rid][tid])
                m.addGenConstrIndicator(D[qid][rid][tid], True, sum(s_over_t) == 1)
                m.addGenConstrIndicator(D[qid][rid][tid], False, sum(s_over_t) == 0)

                # add last variable for this table
                var_name = "last_" + str(tid_new)
                Last[qid][rid][tid] = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)

                # create number of out packets for each table
                var_name = "n_" + str(tid_new)
                table_2_n[tid_new] = m.addVar(lb=0, ub=GRB.INFINITY, vtype=GRB.INTEGER, name=var_name)

                n_over_tid = [F[qid][rid][rid_prev] * cost_matrix[qid][(rid_prev, rid)][tid][-1] for rid_prev in
                              F[qid][rid].keys()]
                m.addGenConstrIndicator(Last[qid][rid][tid], True, table_2_n[tid_new] >= sum(n_over_tid))
                m.addGenConstrIndicator(Last[qid][rid][tid], False, table_2_n[tid_new] == 0)

            # relate d and last variables for each query
            ind = 1
            for tid in query_2_tables[qid]:
                tmp = [D[qid][rid][tid1] for tid1 in query_2_tables[qid][:ind]]
                m.addConstr(D[qid][rid][tid] >= Last[qid][rid][tid])
                ind += 1

            for (tid1, tid2) in zip(query_2_tables[qid][:-1], query_2_tables[qid][1:]):
                m.addConstr(D[qid][rid][tid1] >= D[qid][rid][tid2])

            # inter-query dependency
            # We need at least difference of two stages not one (one stage is required for
            # performing indexing/map operation)
            for (tid1, tid2) in zip(query_2_tables[qid][:-1], query_2_tables[qid][1:]):
                sigma1 = Sigma[qid][rid][tid1]
                sigma2 = Sigma[qid][rid][tid2]
                m.addGenConstrIndicator(D[qid][rid][tid2], True, sigma1 + 1 <= sigma2)

            # Also, the first stateful table cannot start before first two stages
            tid0 = query_2_tables[qid][0]
            sigma0 = Sigma[qid][rid][tid0]
            m.addGenConstrIndicator(D[qid][rid][tid0], True, sigma0 >= 3)

            # create a query (qid, rid) specific count for number of out packets
            var_name = "n_" + str(qid) + "_" + str(rid)
            query_2_n[qid][rid] = m.addVar(lb=0, ub=GRB.INFINITY, vtype=GRB.INTEGER, name=var_name)
            n_over_query = [table_2_n[tid_new] for tid_new in table_2_n.keys()]

            n_over_qr_no_dp = []
            for rid_prev in F[qid][rid].keys():
                tid_min = min(cost_matrix[qid][(rid_prev, rid)].keys())
                n_over_qr_no_dp = [F[qid][rid][rid_prev] * cost_matrix[qid][(rid_prev, rid)][tid_min][0] for rid_prev in
                                   F[qid][rid].keys()]

            var_name = "ind_" + str(qid) + str(rid)
            tmp_ind = m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=var_name)
            m.addConstr(tmp_ind == 1 - sum([Last[qid][rid][tid] for tid in query_2_tables[qid]]))

            # # relate M and Last
            # m.addGenConstrIndicator(tmp_ind, False, ML[qid][rid] >= 1)
            # m.addGenConstrIndicator(tmp_ind, True, ML[qid][rid] <= 0)
            m.addConstr(ML[qid][rid] >= sum([Last[qid][rid][tid] for tid in query_2_tables[qid]]))

            var_name = "qrn_" + str(qid) + "_" + str(rid)
            qrn = m.addVar(lb=0, ub=GRB.INFINITY, vtype=GRB.INTEGER, name=var_name)
            m.addGenConstrIndicator(tmp_ind, True, qrn >= sum(n_over_qr_no_dp))
            m.addGenConstrIndicator(tmp_ind, False, qrn >= sum(n_over_query))

            m.addGenConstrIndicator(I[qid][rid], True, query_2_n[qid][rid] >= qrn)
            m.addGenConstrIndicator(I[qid][rid], True, M[qid][rid] >= 1)
            m.addGenConstrIndicator(I[qid][rid], False, M[qid][rid] <= 0)
            # m.addConstr(ML[qid][rid] >= M[qid][rid])

            # sum of all Last variables is 1 for each (qid, rid)
            l_over_query = [Last[qid][rid][tid] for tid in query_2_tables[qid]]
            m.addConstr(sum(l_over_query) <= 1)

    # apply the pipeline width constraint
    for sid in range(1, 1 + sigma_max):
        s_over_stage = []
        for qid in Q:
            for rid in qid_2_R[qid][1:]:
                s_over_stage += [S[qid][rid][tid][sid] for tid in query_2_tables[qid]]
        m.addConstr(sum(s_over_stage) <= width_max)

    # apply the bits per stage constraint
    BS = {}
    All_BS = {}
    for sid in range(1, 1 + sigma_max):
        BS[sid] = {}
        All_BS[sid] = []
        for qid in Q:
            BS[sid][qid] = {}
            for rid in qid_2_R[qid][1:]:
                BS[sid][qid][rid] = {}
                for tid in query_2_tables[qid]:
                    var_name = "bs_" + str(sid) + "_" + str(qid) + "_" + str(rid) + "_" + str(tid)
                    # maximum bits that a single register can support
                    BS[sid][qid][rid][tid] = m.addVar(lb=0, ub=bits_max_register, vtype=GRB.INTEGER, name=var_name)
                    b_over_r = [F[qid][rid][rid_prev] * cost_matrix[qid][(rid_prev, rid)][tid][1] for rid_prev in
                                F[qid][rid].keys()]
                    m.addGenConstrIndicator(S[qid][rid][tid][sid], True, BS[sid][qid][rid][tid] == sum(b_over_r))
                    m.addGenConstrIndicator(S[qid][rid][tid][sid], False, BS[sid][qid][rid][tid] == 0)
                    All_BS[sid].append(BS[sid][qid][rid][tid])
        m.addConstr(sum(All_BS[sid]) <= bits_max_stage)

    # define the objective, i.e. minimize the total number of packets to send to stream processor
    total_packets = []
    for qid in Q:
        for rid in qid_2_R[qid][1:]:
            total_packets.append(query_2_n[qid][rid])

    m.setObjective(sum(total_packets), GRB.MINIMIZE)

    # Apply the metadata constraint sum_{q} sum_{r} I[q][r] <= M
    tmp_metadata = []
    for qid in Q:
        for rid in qid_2_R[qid][1:]:
            # 64 bit if the executed in data plane, else add 16 bit query identifier
            tmp_metadata.append(16*M[qid][rid]+48*ML[qid][rid])

    m.addConstr(sum(tmp_metadata) <= M_max)

    # Apply join query constraint I_{l,r} = I_{m,r} where queries l and m belong to the same query tree.
    for qid_origin in join_queries:
        qid_pairs = list(itertools.combinations(join_queries[qid_origin], 2))
        for q1, q2 in qid_pairs:
            if q1 in Q and q2 in Q:
                for rid in qid_2_R[q1][1:]:
                    if rid in qid_2_R[q2][1:]:
                        m.addConstr(I[q1][rid] == I[q2][rid])
                    else:
                        print("This should not happen")

    # if mode == 6:
    #     for qid in Q:
    #         tmp = [I[qid][rid] for rid in qid_2_R[qid][1:]]
    #         m.addConstr(sum(tmp) <= 4)

    # Apply mode-specific changes
    if mode in [1, 2]:
        # set all d variables to zero, i.e. no stateful operation in the data plane
        for qid in Q:
            for rid in qid_2_R[qid][1:]:
                if rid != qid_2_R[qid][-1]:
                    m.addConstr(I[qid][rid] == 0)
                for tid in query_2_tables[qid]:
                    m.addConstr(D[qid][rid][tid] == 0)

    elif mode in [3]:
        # deactivate all queries that run at coarser refinement level to zero,
        # i.e. only partitioning at finest refinement level and no iterative refinement
        for qid in Q:
            for rid in qid_2_R[qid][1:-1]:
                m.addConstr(I[qid][rid] == 0)

    elif mode in [4]:
        # activate all queries as all refinement levels to one,
        # i.e. each query uses all possible refinement levels
        for qid in Q:
            for rid in qid_2_R[qid][1:]:
                m.addConstr(I[qid][rid] == 1)

    m.write(name + ".lp")
    # m.setParam(GRB.Param.OutputFlag, 0)
    # m.setParam(GRB.Param.LogToConsole, 0)
    m.setParam(GRB.Param.TimeLimit, 600)

    m.optimize()

    # for v in m.getVars():
    #     print(v.varName, v.x)

    # Print the Output
    out_table = []
    refinement_levels = {}
    table_headers = ["Queries"]
    for sid in range(1, sigma_max + 1):
        table_headers.append(str(sid))

    row_id = 0
    for qid in Q:
        refinement_levels[qid] = "0"
        for rid in qid_2_R[qid][1:]:
            if float(I[qid][rid].x) > 0.5:
                refinement_levels[qid] += "-->" + str(rid)
            out_table.append([])
            out_table[row_id].append("Q" + str(qid) + "/" + str(rid))
            for sid in range(1, sigma_max + 1):
                flag = 0
                for tid in query_2_tables[qid]:
                    # print(qid, rid, sid, tid, S[qid][rid][tid][sid].x, math.ceil(float(S[qid][rid][tid][sid].x)),
                    #       math.ceil(float(S[qid][rid][tid][sid].x)) == 1)
                    if float(S[qid][rid][tid][sid].x) > 0.5 and float(I[qid][rid].x) > 0.5:
                        for rid_prev in F[qid][rid].keys():
                            if float(F[qid][rid][rid_prev].x) > 0.5:
                                out_table[row_id].append(cost_matrix[qid][(rid_prev, rid)][tid][1])
                                flag = 1
                if flag == 0:
                    out_table[row_id].append(0)
            row_id += 1

    meta_size = 0
    for qid in M:
        for rid in M[qid]:
            # print(qid, rid, M[qid][rid].x, ML[qid][rid].x, [Last[qid][rid][tid].x for tid in query_2_tables[qid]])
            if M[qid][rid].x > 0.5:
                meta_size += 16
            if ML[qid][rid].x > 0.5:
                meta_size += 48


    print("Metadata Size:", meta_size)



    print("## Mode", mode)
    print("N(Tuples)", m.objVal)
    print(tabulate(out_table, headers=table_headers))
    print(refinement_levels)
    print("==========================")

    return m, refinement_levels, Last


def test_lp(test_id=1):
    if test_id == 1:
        # Test 1:
        Q = [1]
        query_2_tables = {1: [1, 2]}

        sigma_max = 3
        width_max = 4
        bits_max = 100

        cost_matrix = {1: {(0, 32): {1: (1000, 110), 2: (120, 80)},
                           (0, 16): {1: (1000, 50), 2: (50, 30)},
                           (16, 32): {1: (500, 70), 2: (70, 50)}}
                       }

        qid_2_R = {1: [0, 16, 32]}
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max)
        assert (m.objVal == 80)

    elif test_id == 2:
        # Test 2:
        Q = [1, 2]
        query_2_tables = {1: [1, 2], 2: [1, 2]}

        sigma_max = 1
        width_max = 1
        bits_max = 100

        cost_matrix = {1: {(0, 32): {1: (1000, 50), 2: (50, 40)}},
                       2: {(0, 32): {1: (800, 40), 2: (40, 30)}}
                       }

        qid_2_R = {1: [0, 32], 2: [0, 32]}
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max)
        # for v in m.getVars():
        #     print(v.varName, v.x)
        assert (m.objVal == 850)

    elif test_id == 3:

        sigma_max = 2
        width_max = 2
        bits_max = 100

        Q = [1, 2]
        query_2_tables = {1: [1, 2], 2: [1, 2]}

        cost_matrix = {1: {(0, 32): {1: (1000, 110), 2: (110, 80)},
                           (0, 8): {1: (1000, 40), 2: (40, 20)},
                           (0, 16): {1: (1000, 70), 2: (70, 50)},
                           (8, 16): {1: (700, 60), 2: (60, 40)},
                           (8, 32): {1: (700, 70), 2: (70, 50)},
                           (16, 32): {1: (500, 55), 2: (55, 35)}},
                       2: {(0, 32): {1: (900, 100), 2: (100, 70)},
                           (0, 8): {1: (900, 30), 2: (30, 10)},
                           (0, 16): {1: (900, 50), 2: (60, 30)},
                           (8, 16): {1: (600, 60), 2: (60, 40)},
                           (8, 32): {1: (600, 60), 2: (60, 40)},
                           (16, 32): {1: (300, 45), 2: (55, 25)}}
                       }

        qid_2_R = {1: [0, 8, 16, 32], 2: [0, 8, 16, 32]}

        # set mode to FILTER-ONLY
        mode = 2
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        assert (m.objVal == 1900)

        # set mode to PART-ONLY
        mode = 3
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        assert (m.objVal == 1070)

        # set mode to FIXED-REF
        mode = 4
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        assert (m.objVal == 980)

        # set mode to SONATA
        mode = 6
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        assert (m.objVal == 200)

    elif test_id == 4:

        sigma_max = 2
        width_max = 1
        bits_max = 100

        Q = [1]
        query_2_tables = {1: [1]}

        cost_matrix = {1: {
            (0, 8): {1: (1000, 50)},
            (0, 16): {1: (1000, 60)},
            (0, 24): {1: (1000, 70)},
            (0, 32): {1: (1000, 120)},
            (8, 16): {1: (700, 50)},
            (8, 24): {1: (700, 60)},
            (8, 32): {1: (700, 80)},
            (16, 24): {1: (500, 40)},
            (16, 32): {1: (500, 50)},
            (24, 32): {1: (300, 30)}
        }
        }

        qid_2_R = {1: [0, 8, 16, 24, 32]}

        # set mode to FILTER-ONLY
        mode = 2
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        # assert (m.objVal == 1900)

        # set mode to PART-ONLY
        mode = 3
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        # assert (m.objVal == 1070)

        # set mode to FIXED-REF
        mode = 4
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        # assert (m.objVal == 980)

        # set mode to SONATA
        mode = 6
        m = solve_sonata_lp(Q, query_2_tables, cost_matrix, qid_2_R, sigma_max, width_max, bits_max, mode)
        # assert (m.objVal == 200)


if __name__ == '__main__':
    test_lp(4)
