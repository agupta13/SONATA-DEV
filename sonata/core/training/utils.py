import pickle

from netaddr import *
import math

# from sonata.system_config import *
from sonata.query_engine.sonata_queries import *
import sonata.streaming_driver.query_object as spark
from sonata.core.training.hypothesis.costs.dp_cost import get_data_plane_cost
from sonata.core.training.hypothesis.costs.sp_cost import get_streaming_cost


def parse_log_line(logline):
    return tuple(logline.split(","))


def shard_training_data(sc, flows_File, T):
    training_data = (sc.textFile(flows_File)
                     .map(parse_log_line)
                     .map(lambda s: tuple([int(math.ceil(int(s[0]) / T))] + (list(s[1:]))))
                     .filter(lambda (ts, sIP, sPort, dIP, dPort, nBytes, proto, sMac, dMac): str(proto) == '17')
                     # .filter(lambda (ts,sIP,sPort,dIP,dPort,nBytes,proto,sMac,dMac): str(sPort) == '53')
                     )
    print "Collecting the training data for the first time ..."
    training_data = sc.parallelize(training_data.collect())
    print "Collecting timestamps for the experiment ..."
    timestamps = training_data.map(lambda s: s[0]).distinct().collect()
    print "#Timestamps: ", len(timestamps)
    return timestamps, training_data


def add_timestamp_key(qid_2_query):
    def add_timestamp_to_query(q):
        # This function will be useful if we need to add ts in recursion
        for operator in q.operators:
            operator.keys = tuple(list(operator.keys))
            # operator.keys = tuple(['ts'] + list(operator.keys))

    for qid in qid_2_query:
        query = qid_2_query[qid]
        add_timestamp_to_query(query)
    return qid_2_query


# def generate_intermediate_sonata_queries(sonata_query, refinement_level):
#     number_intermediate_queries = len(filter(lambda s: s in ['Distinct', 'Reduce', 'Filter'], [x.name for x in sonata_query.operators]))
#     sonata_intermediate_queries = {}
#     prev_qid = 0
#     filter_mappings = {}
#     filters_marked = {}
#     for max_operators in range(1,1+number_intermediate_queries):
#         qid = (1000*sonata_query.qid)+max_operators
#         tmp_query = (PacketStream(sonata_query.qid))
#         tmp_query.basic_headers = BASIC_HEADERS
#         ctr = 0
#         filter_ctr = 0
#         prev_operator = None
#         for operator in sonata_query.operators:
#             if operator.name != 'Join':
#                 if ctr < max_operators:
#                     copy_operators(tmp_query, operator)
#                     prev_operator = operator
#                 else:
#                     break
#                 if operator.name in ['Distinct', 'Reduce', 'Filter']:
#                     ctr += 1
#                 if operator.name == 'Filter':
#                     filter_ctr += 1
#                     if (qid, refinement_level, filter_ctr) not in filters_marked:
#                         filters_marked[(qid, refinement_level, filter_ctr)] = sonata_query.qid
#                         filter_mappings[(prev_qid, qid, refinement_level)] = (sonata_query.qid, filter_ctr, operator.func[1])
#             else:
#                 prev_operator = operator
#                 copy_operators(tmp_query, operator)
#
#         sonata_intermediate_queries[qid] = tmp_query
#         prev_qid = qid
#
#     return sonata_intermediate_queries, filter_mappings

def get_partition_plans_learning(spark_query, target):
    # receives dp_query object.
    total_operators = len(spark_query.operators)
    partition_plans_learning = []
    ctr = 1
    for operator in spark_query.operators:
        can_increment = True
        if operator.name in target.supported_operators.keys():
            if hasattr(operator, 'func') and len(operator.func) > 0:
                if operator.func[0] in target.supported_operators[operator.name]:
                    if operator.name in target.learning_operators:
                        partition_plans_learning.append(ctr)
                else:
                    break
            else:
                if operator.name in target.learning_operators:
                    partition_plans_learning.append(ctr)
        else:
            break

        if operator.name == 'Map':
            if hasattr(operator, 'func') and len(operator.func) > 0:
                if operator.func[0] == 'mask':
                    can_increment = False

        if can_increment:
            ctr += 1
            # print operator.name, partition_plans_learning

    return partition_plans_learning


def generate_intermediate_spark_queries(spark_query, refinement_level, target):
    partition_plans_learning = get_partition_plans_learning(spark_query, target)
    if spark_query.operators[-1].name != 'Filter':
        partition_plans_learning += [len(spark_query.operators)]
    spark_intermediate_queries = {}
    prev_qid = 0
    filter_mappings = {}
    filters_marked = {}
    for max_operators in partition_plans_learning:
        qid = 1000 * spark_query.qid + max_operators
        tmp_query = (spark.PacketStream(spark_query.qid))
        tmp_query.basic_headers = BASIC_HEADERS
        ctr = 0
        filter_ctr = 0
        prev_operator = None
        for operator in spark_query.operators:
            can_increment = True
            if operator.name != 'Join':
                if ctr < max_operators:
                    copy_spark_operators_to_spark(tmp_query, operator)
                    prev_operator = operator
                else:
                    break
                if operator.name == 'Filter':
                    filter_ctr += 1
                    if (qid, refinement_level, filter_ctr) not in filters_marked:
                        filters_marked[(qid, refinement_level, filter_ctr)] = spark_query.qid
                        filter_mappings[(prev_qid, qid, refinement_level)] = (
                        spark_query.qid, filter_ctr, operator.func[1])
            else:
                copy_sonata_operators_to_spark(tmp_query, operator)
                prev_operator = operator
            if operator.name == 'Map':
                if hasattr(operator, 'func') and len(operator.func) > 0:
                    if operator.func[0] == 'mask':
                        can_increment = False
            if can_increment:
                ctr += 1

        spark_intermediate_queries[qid] = tmp_query
        prev_qid = qid

    return spark_intermediate_queries, filter_mappings


def generate_query_to_collect_transit_cost(transit_query_string, spark_query):
    # print spark_query, spark_query.operators
    if len(spark_query.operators) > 0:
        last_operator_name = spark_query.operators[-1].name
        if last_operator_name == 'Reduce':
            # transit_query_string += '.map(lambda s: (s[0][0], s[1])).groupByKey().map(lambda s: (s[0], list(s[1])))'

            # only focus on counts right now.
            transit_query_string += '.map(lambda s: (s[0][0], 1)).reduceByKey(lambda x,y: x+y)'
        else:
            if last_operator_name == 'Distinct':
                transit_query_string += '.map(lambda s: (s[0], 1)).reduceByKey(lambda x,y: x+y)'
            else:
                transit_query_string += '.map(lambda s: (s[0][0], 1)).reduceByKey(lambda x,y: x+y)'
    else:
        transit_query_string += '.map(lambda s: (s[0], 1)).reduceByKey(lambda x,y: x+y)'

    transit_query_string += '.collect()'
    return transit_query_string


def generate_transit_query(curr_query, curr_level_out_fname, prev_level_out_mapped_string, ref_level_prev, refinement_key):
    refinement_key = refinement_key.replace(".", "_")
    if len(curr_query.operators) > 0:
        keys = curr_query.operators[-1].keys
        values = curr_query.operators[-1].values
    else:
        keys = BASIC_HEADERS
        values = ()

    transit_query_string = 'load_rdd(curr_level_out_fname, self.sc)'
    if len(values) > 0:
        transit_query_string += '.map(lambda ((' + ",".join(keys) + '),(' + ",".join(values) + ')):'
        transit_query_string += '((ts, str(IPNetwork(str('+refinement_key+')+"/"+str(' + str(ref_level_prev) + ')).network)),'
        transit_query_string += '((' + ",".join(keys) + '),(' + ",".join(values) + '))))'
    else:
        transit_query_string += '.map(lambda (' + ",".join(keys) + '): '
        transit_query_string += '((ts, str(IPNetwork(str('+refinement_key+')+"/"+str(' + str(
            ref_level_prev) + ')).network)),(' + ",".join(keys) + ')))'
    transit_query_string += '.join('+prev_level_out_mapped_string+').map(lambda x: x[1][0])'
    transit_query_string = generate_query_to_collect_transit_cost(transit_query_string, curr_query)
    # print transit_query_string
    return transit_query_string


def generate_query_string_prev_level_out_mapped(qid, ref_level_prev, query_out_refinement_level, refined_spark_queries,
                                                out0_fname, reduction_key):
    reduction_key = reduction_key.replace(".", "_")
    if ref_level_prev > 0:
        iter_qids_prev = query_out_refinement_level[qid][ref_level_prev].keys()
        iter_qids_prev.sort()
        prev_level_out_fname = query_out_refinement_level[qid][ref_level_prev][iter_qids_prev[-1]]
        prev_query = refined_spark_queries[qid][ref_level_prev][iter_qids_prev[-1]]
    else:
        prev_level_out = out0_fname

        # We need to filter out result from `curr_level_out` that is not in `prev_level_out`
    if len(prev_query.operators) > 0:
        keys = prev_query.operators[-1].keys
        values = prev_query.operators[-1].values
    else:
        keys = BASIC_HEADERS
        values = ()
    prev_level_out_mapped_string = 'load_rdd(prev_level_out_fname, self.sc)'
    prev_level_out_mapped_string += '.map(lambda ((' + ",".join(keys) + '),(' + ",".join(values) + ')):'
    # we should apply distinct here at the end so that we have distinct masked keys as output
    # otherwise the join will result in cartesian and we will have duplicate entries for the finer ref level as input
    prev_level_out_mapped_string += '((ts,' + str(reduction_key) + '), 1)).distinct()'

    print "prev_level_out_mapped_string", prev_level_out_mapped_string, "prev_level", ref_level_prev

    return prev_level_out_mapped_string, prev_level_out_fname


def dump_data(data, fname):
    with open(fname, 'w') as f:
        print "Dumping query counts ..." + fname
        pickle.dump(data, f)


def update_counts(sc, queries, query_out, iter_qid, delta, bits_count, packet_count, ctr):
    curr_operator = queries[iter_qid].operators[-1]
    curr_query_out = query_out[iter_qid]

    if curr_operator.name in ['Distinct', 'Reduce']:
        # Update the number of bits required to perform this operation

        if curr_operator.name == 'Reduce':
            next_operator = queries[iter_qid + 1].operators[-1]
            next_query_out = query_out[iter_qid + 1]
            thresh = 1
            if next_operator.name == 'Filter':
                thresh = int(next_operator.func[1])
                packet_count = get_streaming_cost(sc, curr_operator.name, next_query_out)

            delta_bits = get_data_plane_cost(sc, curr_operator.name, curr_operator.func[0],
                                             curr_query_out, thresh, delta)

        else:
            # for 'Distinct' operator
            thresh = 1
            delta_bits = get_data_plane_cost(sc, curr_operator.name, '',
                                             curr_query_out, thresh, delta)
            packet_count = get_streaming_cost(sc, curr_operator.name, curr_query_out)

        bits_count = bits_count.join(delta_bits).map(lambda s: (s[0], (s[1][0] + s[1][1])))

        print "After executing ", curr_operator.name, " in Data Plane"
        print "Bits Count Cost", bits_count.collect()[:2]
        print "Packet Count Cost", packet_count.collect()[:2]


        ctr += 1
    return bits_count, packet_count, ctr


def create_spark_context():
    from pyspark import SparkContext, SparkConf
    conf = (SparkConf()
            .setMaster("local[*]")
            .setAppName("SONATA-Training")
            .set("spark.executor.memory", "10g")
            .set("spark.driver.memory", "20g")
            .set("spark.cores.max", "16"))

    sc = SparkContext(conf=conf)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    return sc


# def get_spark_context_batch(sc):
#     # Load training data
#     timestamps, training_data_fname = shard_training_data(sc, TD_PATH, T)
#     return timestamps, training_data_fname
