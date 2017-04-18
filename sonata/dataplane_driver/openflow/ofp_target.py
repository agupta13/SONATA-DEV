#!/usr/bin/env python


from ofp_application import OFPApplication
from ofp_dataplane import OFPDataPlane

from threading import Thread

from sonata.dataplane_driver.utils import get_logger
from sonata.dataplane_driver.openflow.emitter.emitter import Emitter


class OFPTarget(object):
    def __init__(self, em_conf):
        self.supported_operations = ['Filter']

        self.em_conf = em_conf

        # interfaces
        self.interfaces = {
                'receiver': ['m-veth-1', 'out-veth-1'],
                'sender': ['m-veth-2', 'out-veth-2']
        }

        # LOGGING
        self.logger = get_logger('P4Target', 'INFO')
        self.logger.info('init')

        # init dataplane
        self.dataplane = OFPDataPlane(self.interfaces)

        # p4 app object
        self.app = None

    def get_supported_operators(self):
        return self.supported_operations

    def run(self, app):
        self.logger.info('run')

        # turn application into flow rules
        self.logger.info('init ofp application object')
        self.app = OFPApplication(app)

        # initialize dataplane and install flow rules for default forwarding
        self.logger.info('initialize the dataplane')
        self.dataplane.initialize()

        # push flow rules of ofp application
        flow_rules = self.app.get_flow_rules(self.dataplane.get_emitter_port())
        self.dataplane.push_flow_rules(flow_rules)

        # start the emitter
        if self.em_conf:
            self.logger.info('start the emitter')
            em = Emitter(self.em_conf, self.app.get_id_to_qid_mapping(), self.app.get_out_headers())
            em_thread = Thread(name='emitter', target=em.start)
            em_thread.setDaemon(True)
            em_thread.start()

    def update(self, filter_update):
        self.logger.info('update')

        # Get the commands to add new filter flow rules
        flow_rules = self.app.get_update_flow_rules(self.dataplane.get_emitter_port(), filter_update)
        self.dataplane.push_flow_rules(flow_rules)
