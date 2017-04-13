#!/usr/bin/env python
# Author: Ruediger Birkner (Networked Systems Group at ETH Zurich)


from sonata.dataplane_driver.utils import get_logger


HEADER_MAP = {'sIP': 'ipv4.srcAddr', 'dIP': 'ipv4.dstAddr',
              'sPort': 'tcp.srcPort', 'dPort': 'tcp.dstPort',
              'nBytes': 'ipv4.totalLen', 'proto': 'ipv4.protocol',
              'sMac': 'ethernet.srcAddr', 'dMac': 'ethernet.dstAddr', 'payload': ''}

HEADER_SIZE = {'sIP': 32, 'dIP': 32, 'sPort': 16, 'dPort': 16,
               'nBytes': 16, 'proto': 16, 'sMac': 48, 'dMac': 48,
               'qid': 16, 'count': 16}


# Class that holds one refined query - which consists of an ordered list of operators
class OFPQuery(object):
    def __init__(self, query_id, generic_operators):

        # LOGGING
        self.logger = get_logger('OFPQuery - %i' % query_id, 'INFO')
        self.logger.info('init')

        for operator in generic_operators:
            pass
            # add the operators
