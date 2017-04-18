#!/usr/bin/env python
# Author: Ruediger Birkner (Networked Systems Group at ETH Zurich)


from ofp_query import OFPQuery
from sonata.dataplane_driver.utils import get_logger
from sonata.dataplane_driver.openflow.ofp_flowmod_msg import FlowModMsgBuilder


class OFPApplication(object):
    def __init__(self, app):
        # LOGGING
        self.logger = get_logger('OFPApplication', 'INFO')
        self.logger.info('init')

        self.id_to_qid = dict()
        self.qid_to_id = dict()
        self.queries = dict()

    # INIT THE DATASTRUCTURE
    def init_application(self, app):
        # transforms queries
        for tmp_table_id, query_id in enumerate(app):
            table_id = tmp_table_id + 1
            self.logger.debug('create filter for qid: %i' % (query_id, ))
            operators = app[query_id].operators
            query = OFPQuery(query_id,
                             table_id,
                             operators)
            self.queries[query_id] = query

            self.id_to_qid[table_id] = query_id
            self.qid_to_id[query_id] = table_id

    def get_flow_rules(self, emitter_port):
        fm_builder = FlowModMsgBuilder()

        for qid, query in self.queries.iteritems():
            table_id = self.qid_to_id[qid]
            next_table_id = table_id + 1 if table_id < len(self.queries) else -1
            match, action, cookie = query.get_flow_rules(emitter_port, next_table_id)

            fm_builder.add_flow_mod('insert', 'filter_%i' % table_id, 5, match, action, cookie)

        return fm_builder.get_msg()

    def get_update_flow_rules(self, emitter_port, filter_update):
        fm_builder = FlowModMsgBuilder()

        for qid, filter_id in filter_update:
            table_id = self.qid_to_id[qid]
            query = self.queries[qid]
            next_table_id = table_id + 1 if table_id < len(self.queries) else -1

            match, action, cookie = query.get_update_flow_rules(emitter_port, next_table_id, filter_update[filter_id])

            # delete old filter entries
            fm_builder.delete_flow_mod('remove', 'filter_%i' % table_id, cookie)
            # create new ones
            fm_builder.add_flow_mod('insert', 'filter_%i' % table_id, 5, match, action, cookie)
        return fm_builder.get_msg()

    def get_id_to_qid_mapping(self):
        return self.id_to_qid

    def get_out_headers(self):
        out_headers = dict()
        for qid, query in self.queries.iteritems():
            out_headers[qid] = query.get_out_headers()
