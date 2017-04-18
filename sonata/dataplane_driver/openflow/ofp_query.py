#!/usr/bin/env python
# Author: Ruediger Birkner (Networked Systems Group at ETH Zurich)


from sonata.dataplane_driver.utils import get_logger

# TODO Need to distinguish TCP and UDP ports!
HEADER_MAP = {'sIP': 'ipv4_src', 'dIP': 'ipv4_dst', 'sPort': 'tcp_src',
              'dPort': 'tcp_dst', 'proto': 'ip_proto', 'sMac': 'eth_src',
              'dMac': 'eth_dst'}


# Class that holds one refined query - which consists of a single filter
class OFPQuery(object):
    def __init__(self, query_id, table_id, filter_operator):

        # LOGGING
        self.logger = get_logger('OFPQuery - %i' % query_id, 'INFO')
        self.logger.info('init')

        # QUERY SPECIFICS
        self.id = query_id
        self.table_id = table_id
        self.filter_values = list()
        self.filter_mask = ''
        self.func = ''

        if filter_operator.name == 'Filter':
            if not len(filter_operator.func) > 0 or filter_operator.func[0] == 'geq':
                self.logger.error('Got the following func with the Filter Operator: %s' % (str(filter_operator.func),))
                # raise NotImplementedError
            else:
                self.func = filter_operator.func[0]
                if filter_operator.func[0] == 'mask':
                    self.filter_mask = filter_operator.func[1]
                    self.filter_values = filter_operator.func[2:]
                elif filter_operator.func[0] == 'eq':
                    self.filter_values = filter_operator.func[1:]

            self.filter_keys = list()
            for filter_key in filter_operator.filter_keys:
                self.filter_keys.append(filter_key)
                break

    def get_flow_rules(self, emitter_port, next_table_id):
        match = dict()
        # TODO Need to distinguish TCP and UDP ports!
        for filter_value in self.filter_values:
            match[HEADER_MAP[filter_value]] = filter_value
        action = {'fwd': [emitter_port, 'filter_%i' % next_table_id], 'set_eth_src': self.table_id}
        cookie = self.id

        return match, action, cookie

    def get_update_flow_rules(self, emitter_port, next_table_id, update):
        match = dict()
        for dip in update:
            match['ipv4_dst'] = dip
        action = {'fwd': [emitter_port, 'filter_%i' % next_table_id], 'set_eth_src': self.table_id}
        cookie = self.id

        return match, action, cookie

    def get_out_headers(self):
        pass
        # TODO!!!
