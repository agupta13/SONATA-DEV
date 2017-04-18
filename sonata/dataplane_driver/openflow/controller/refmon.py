#  Author:
#  Rudiger Birkner (Networked Systems Group ETH Zurich)

import logging

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER 
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_0, ofproto_v1_3
from ryu.app.wsgi import WSGIApplication

from ofp13 import FlowMod as OFP13FlowMod
from lib import Config, Controller

from rest import FlowModReceiver

LOG = False


class RefMon(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION, ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(RefMon, self).__init__(*args, **kwargs)

        # Used for REST API
        wsgi = kwargs['wsgi']
        wsgi.register(FlowModReceiver, self)

        self.logger = logging.getLogger('ReferenceMonitor')
        self.logger.info('refmon: start')

        # configuration of dataplane
        tables = {
            'forwarding': 0,
        }
        for i in range(1, 101):
            tables['filter_%i' % i] = i

        connections = {
            'main': {
                'source': 11,
                'emitter': 12
            }
        }

        short_url = 'refmon/flow_mods'
        self.config = Config(tables, connections, short_url)

        # start controller
        self.controller = Controller(self.config)

    def close(self):
        self.logger.info('refmon: stop')

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def dp_state_change_handler(self, ev):
        datapath = ev.datapath

        if ev.state == MAIN_DISPATCHER:
            self.controller.switch_connect(datapath)
        elif ev.state == DEAD_DISPATCHER:
            self.controller.switch_disconnect(datapath)
        
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        self.controller.packet_in(ev)

    def process_flow_mods(self, msg):
        self.logger.info('refmon: received flowmod request')

        if "flow_mods" in msg:
            # push flow mods to the data plane
            self.logger.info('refmon: process ' + str(len(msg["flow_mods"])) + ' flowmods')
            for flow_mod in msg["flow_mods"]:
                fm = OFP13FlowMod(self.config, flow_mod)
                self.controller.process_flow_mod(fm)
