#!/usr/bin/env python
# Author: Ruediger Birkner (Networked Systems Group at ETH Zurich)


from ofp_query import OFPQuery
from sonata.dataplane_driver.utils import get_logger


class OFPApplication(object):
    def __init__(self, app):
        # LOGGING
        self.logger = get_logger('OFPApplication', 'INFO')
        self.logger.info('init')

    # INIT THE DATASTRUCTURE
    def init_application(self, app):
        queries = dict()

        # transforms queries
        for query_id in app:
            self.logger.debug('create query pipeline for qid: %i' % (query_id, ))
            parse_payload = app[query_id].parse_payload
            operators = app[query_id].operators
            query = OFPQuery(query_id,
                            operators)
            queries[query_id] = query

        return queries

    # COMPILE THE CODE
    def compile(self):
        pass
