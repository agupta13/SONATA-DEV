#!/usr/bin/env python
#  Author:
#  Arpit Gupta (arpitg@cs.princeton.edu)

import logging
import pickle
import time
from multiprocessing.connection import Client, Listener
from threading import Thread

from sonata.core.training.hypothesis.hypothesis import Hypothesis
from sonata.streaming_driver.streaming_driver import StreamingDriver

# from sonata.core.training.weights.training_data_fname import TrainingData
from sonata.core.training.utils import get_spark_context_batch, create_spark_context

from sonata.core.training.learn.learn import Learn
from sonata.core.refinement import apply_refinement_plan, get_refined_query_id, Refinement
from sonata.core.partition import get_dataplane_query, get_streaming_query

from sonata.core.integration import Target
from sonata.dataplane_driver.dp_driver import DataplaneDriver


class Runtime(object):
    dp_queries = {}
    sp_queries = {}
    query_plans = {}
    refinement_keys = {}
    query_in_mappings = {}
    query_out_mappings = {}
    query_out_final = {}
    op_handler_socket = None
    op_handler_listener = None

    def __init__(self, conf, queries):
        self.conf = conf
        self.refinement_keys = conf["refinement_keys"]
        self.queries = queries
        self.initialize_logging()
        self.target_id = 1
        # self.sc = create_spark_context()

        use_pickled_queries = False
        if use_pickled_queries:
            with open('pickled_queries.pickle', 'r') as f:
                pickled_queries = pickle.load(f)
                self.dp_queries = pickled_queries[0]
                self.sp_queries = pickled_queries[1]
        else:
            # (self.timestamps, self.training_data_fname) = get_spark_context_batch(self.sc)
            # Learn the query plan
            for query in self.queries:
                target = Target()
                assert hasattr(target, 'costly_operators')
                refinement_object = Refinement(query, target, self.refinement_keys)

                # self.refinement_keys[query.qid] = refinement_object.refinement_key
                print "*********************************************************************"
                print "*                   Generating Query Plan                           *"
                print "*********************************************************************\n\n"
                # fname = "plan_" + str(query.qid) + ".pickle"
                # usePickledPlan = True
                # if usePickledPlan:
                #     with open(fname, 'r') as f:
                #         self.query_plans[query.qid] = pickle.load(f)
                # else:
                #     # update the threshold for the refined queries
                #     refinement_object.update_filter(self.training_data_fname)
                #     # Generate hypothesis graph for each query
                #     # query, sc, training_data_fname, timestamps, refinement_object
                #     hypothesis = Hypothesis(query, self.sc, self.training_data_fname, self.timestamps,
                #                             refinement_object, target)
                #
                #     # Learn the query plan using the hypothesis graphs
                #     learn = Learn(hypothesis)
                #     self.query_plans[query.qid] = [x.state for x in learn.final_plan.path]
                #     with open(fname, 'w') as f:
                #         pickle.dump(self.query_plans[query.qid], f)
                #
                # # Generate queries for the data plane and stream processor after learning the final plan
                # final_plan = self.query_plans[query.qid][1:-1]
                # print final_plan

                final_plan = [(1, 16, 5, 1), (3, 32, 1, 2)]  # (1, 16, 5, 1),
                final_plan = conf["final_plan"]# (3, 32, 1, 2)]  # (1, 16, 5, 1),
                # final_plan = [(1, 32, 5, 1)]
                prev_r = 0
                prev_qid = 0

                for (q, r, p, l) in final_plan:
                    qry = refinement_object.qid_2_query[q]
                    refined_query_id = get_refined_query_id(qry, r)

                    refined_sonata_query = refinement_object.get_refined_updated_query(qry.qid, r, prev_qid, prev_r)
                    if prev_r > 0:
                        p += 1
                    dp_query = get_dataplane_query(refined_sonata_query, refined_query_id, p)
                    self.dp_queries[refined_query_id] = dp_query

                    sp_query = get_streaming_query(refined_sonata_query, refined_query_id, p)
                    self.sp_queries[refined_query_id] = sp_query
                    prev_r = r
                    prev_qid = q

                print self.dp_queries, self.sp_queries

                self.update_query_mappings(refinement_object, final_plan)

                # final_plan = [(16, 5, 1), (32, 1, 1)]

                # print "# of iteration levels", len(final_plan)
                # prev_r = 0
                # for (r, p, cache) in final_plan:
                #     # Get the query id
                #     refined_query_id = get_refined_query_id(query, r)
                #
                #     # Generate query for this refinement level
                #     refined_sonata_query = refinement_object.get_refined_updated_query(r, prev_r)
                #
                #     if prev_r > 0:
                #         p += 1
                #
                #     # Apply the partitioning plan for this refinement level
                #     dp_query = get_dataplane_query(refined_sonata_query, refined_query_id, p)
                #     self.dp_queries[refined_query_id] = dp_query
                #
                #     # Generate input and output mappings
                #     sp_query = get_streaming_query(refined_sonata_query, refined_query_id, p)
                #     self.sp_queries[refined_query_id] = sp_query
                #
                #     prev_r = r
            # sc.stop()
            with open('pickled_queries.pickle', 'w') as f:
                pickle.dump({0: self.dp_queries, 1: self.sp_queries}, f)

        print "Dataplane Queries", self.dp_queries
        print "\n\n"
        print "Streaming Queries", self.sp_queries

        # time.sleep(10)
        self.initialize_handlers()
        time.sleep(2)
        self.send_to_dp_driver('init', self.dp_queries)
        print "*********************************************************************"
        print "*                   Updating Dataplane Driver                       *"
        print "*********************************************************************\n\n"
        if self.sp_queries:
            self.send_to_sm()

        # self.dp_driver_thread.join()
        self.streaming_driver_thread.join()
        self.op_handler_thread.join()

    def update_query_mappings(self, refinement_object, final_plan):
        if len(final_plan) > 1:
            query1 = refinement_object.qid_2_query[final_plan[0][0]]
            for ((q1, r1, p1, l1), (q2, r2, p2, l2)) in zip(final_plan, final_plan[1:]):
                query1 = refinement_object.qid_2_query[q1]
                query2 = refinement_object.qid_2_query[q2]

                qid1 = get_refined_query_id(query1, r1)
                qid2 = get_refined_query_id(query2, r2)
                if qid2 not in self.query_in_mappings:
                    self.query_in_mappings[qid2] = []
                self.query_in_mappings[qid2].append(qid1)

                if qid1 not in self.query_out_mappings:
                    self.query_out_mappings[qid1] = []
                self.query_out_mappings[qid1].append(qid2)

                # Update the queries whose o/p needs to be displayed to the network operators
            # print final_plan
            r = final_plan[-1][0]
            qid = get_refined_query_id(query1, r)
            self.query_out_final[qid] = 0
        else:
            print "No mapping update required"



    def start_op_handler(self):
        """
        At the end of each window interval, two things need to happen for each query (in order),
        (1) registers and filter tables need to be flushed, (2) Filter tables need to get updated.
        We tried to do (1) and (2) for each query independently, but struggled as there were no
        easy way to flush specific registers for a query. So what we ended up doing was to wait
        for o/p from all queries, use the reset command to flush all registers/tables at once,
        and then update them with the delta commands. I am sure there is a better way of solving
        this reset and update problem.
        """
        # Start the output handler
        # It receives output for each query in SP
        # It sends output of the coarser queries to the dataplane driver or
        # SM depending on where filter operation is applied (mostly DP)
        self.op_handler_socket = tuple(self.conf['sm_conf']['op_handler_socket'])
        self.op_handler_listener = Listener(self.op_handler_socket)

        start = "%.20f" % time.time()

        queries_received = {}
        updateDeltaConfig = False
        while True:
            # print "Ready to receive data from SM ***************************"
            conn = self.op_handler_listener.accept()
            # Expected (qid,[])
            op_data = conn.recv_bytes()
            op_data = op_data.strip('\n')
            # print "$$$$ OP Handler received:" + str(op_data)
            received_data = op_data.split(",")
            src_qid = int(received_data[1])
            if received_data[2:] != ['']:
                table_match_entries = received_data[2:]
                queries_received[src_qid] = table_match_entries
            else:
                queries_received[src_qid] = []
            print "DP Queries: ", str(len(self.dp_queries.keys())), " Received keys:", str(len(queries_received.keys()))
            if len(queries_received.keys()) == len(self.dp_queries.keys()):
                updateDeltaConfig = True

            print "Query Out Mappings: ",self.query_out_mappings
            delta_config = {}
            # print "## Received output for query", src_qid, "at time", time.time() - start
            if updateDeltaConfig:
                start = "%.20f" % time.time()
                for src_qid in queries_received:
                    if src_qid in self.query_out_mappings:
                        table_match_entries = queries_received[src_qid]
                        if len(table_match_entries) > 0:
                            out_queries = self.query_out_mappings[src_qid]
                            for out_qid in out_queries:
                                # find the queries that take the output of this query as input
                                # print out_qid, src_qid
                                delta_config[(out_qid, src_qid)] = table_match_entries
                                # reset these state variables
                # print "delta config: ", delta_config
                updateDeltaConfig = False
                if delta_config != {}: self.logger.info(
                    "runtime,create_delta_config," + str(start) + ",%.20f" % time.time())
                queries_received = {}

            # TODO: Update the send_to_dp_driver function logic
            # now send this delta config to fabric manager and update the filter tables
            if delta_config != {}:
                IP = ""
                for qid_key in delta_config.keys():
                    IP = delta_config[qid_key]

                print "*********************************************************************"
                print "*                   IP " + IP[0] + " satisfies coarser query            *"
                print "*                   Reconfiguring Data Plane                        *"
                print "*********************************************************************\n\n"

                self.send_to_dp_driver("delta", delta_config)
        return 0

    def start_dataplane_driver(self):
        # Start the fabric managers local to each data plane element
        dpd = DataplaneDriver(self.conf['fm_conf']['fm_socket'], self.conf["internal_interfaces"], self.conf['fm_conf']['log_file'])
        self.dpd_thread = Thread(name='dp_driver', target=dpd.start)
        self.dpd_thread.setDaemon(True)

        p4_type = 'p4'

        config = {
            'em_conf': self.conf['emitter_conf'],
            'switch_conf': {
                'compiled_srcs': '/home/vagrant/dev/sonata/dataplane_driver/' + p4_type + '/compiled_srcs/',
                'json_p4_compiled': 'compiled.json',
                'p4_compiled': 'compiled.p4',
                'p4c_bm_script': '/home/vagrant/p4c-bmv2/p4c_bm/__main__.py',
                'bmv2_path': '/home/vagrant/bmv2',
                'bmv2_switch_base': '/targets/simple_switch',
                'switch_path': '/simple_switch',
                'cli_path': '/sswitch_CLI',
                'thriftport': 22222,
                'p4_commands': 'commands.txt',
                'p4_delta_commands': 'delta_commands.txt'
            }
        }
        dpd.add_target(p4_type, self.target_id, config)
        self.dpd_thread.start()

        # fm = DPDriverConfig(self.conf['fm_conf'], self.conf['emitter_conf'])
        # fm.start()
        # while True:
        #     time.sleep(5)
        # return 0

    def start_streaming_driver(self):
        # Start streaming managers local to each stream processor
        # self.conf['sm_conf']['sc']=self.sc
        sm = StreamingDriver(self.conf['sm_conf'])
        sm.start()
        while True:
            time.sleep(5)
        return 0

    def compile(self):
        query_expressions = []
        for query in self.queries:
            query_expressions.append(query.compile_sp())
        return query_expressions

    def send_to_sm(self):
        # Send compiled query expression to streaming manager
        start = "%.20f" % time.time()
        serialized_queries = pickle.dumps(self.sp_queries)
        conn = Client(tuple(self.conf['sm_conf']['sm_socket']))
        conn.send(serialized_queries)
        self.logger.info("runtime,sm_init," + str(start) + "," + str(time.time()))
        print "*********************************************************************"
        print "*                   Updating Streaming Driver                       *"
        print "*********************************************************************\n\n"
        time.sleep(3)

    def send_to_dp_driver(self, message_type, content):
        # Send compiled query expression to fabric manager
        start = "%.20f" % time.time()

        with open('dns_reflection.pickle', 'w') as f:
            pickle.dump(content, f)

        message = {message_type: {0: content, 1: self.target_id}}
        serialized_queries = pickle.dumps(message)
        conn = Client(tuple(self.conf['fm_conf']['fm_socket']))
        conn.send(serialized_queries)
        self.logger.info("runtime,fm_" + message_type + "," + str(start) + ",%.20f" % time.time())
        time.sleep(1)
        conn.close()
        print "*********************************************************************"
        print "*                   Updating Dataplane Driver                       *"
        print "*********************************************************************\n\n"
        return ''

    def initialize_handlers(self):
        target = self.start_dataplane_driver()
        self.streaming_driver_thread = Thread(name='streaming_driver', target=self.start_streaming_driver)
        self.op_handler_thread = Thread(name='op_handler', target=self.start_op_handler)
        # self.fm_thread.setDaemon(True)
        self.streaming_driver_thread.start()
        self.op_handler_thread.start()
        time.sleep(1)

    def initialize_logging(self):
        # print "######Setup Logger##########",self.conf['log_file']
        # create a logger for the object
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        # create file handler which logs messages
        self.fh = logging.FileHandler(self.conf['base_folder'] + self.__class__.__name__)
        self.fh.setLevel(logging.INFO)
        self.logger.addHandler(self.fh)
