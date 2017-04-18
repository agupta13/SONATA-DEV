#!/usr/bin/env python
# Author: Ruediger Birkner (Networked Systems Group at ETH Zurich)


import os
import subprocess
import requests
import json

from time import sleep

from mininet.link import Intf
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch, Node

from sonata.dataplane_driver.p4.interfaces import Interfaces
from sonata.dataplane_driver.utils import get_logger

from sonata.dataplane_driver.openflow.ofp_flowmod_msg import FlowModMsgBuilder


class OFPDataPlane(object):
    def __init__(self, interfaces):
        self.interfaces = interfaces

        # LOGGING
        self.logger = get_logger('OPFDataplane', 'INFO')
        self.logger.info('init')

        # DP PORTS
        self.SOURCE_PORT = 11
        self.EMITTER_PORT = 12

        # URL
        self.RYU_REST_PORT = 3333
        self.url = "http://localhost:%i/refmon/flow_mods" % self.RYU_REST_PORT

    def initialize(self):
        self.logger.info('initialize')

        self.create_interfaces()

        self.logger.info('start mininet topology')
        topo = OFPTopo()

        net = Mininet(topo=topo,
                      switch=OVSSwitch,
                      controller=RemoteController)

        Intf("m-veth-1", net.get('s1'), self.SOURCE_PORT)
        Intf("m-veth-2", net.get('s1'), self.EMITTER_PORT)
        net.start()
        sleep(1)

        # start ryu controller in the background
        controller_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'refmon.py')
        subprocess.Popen(['ryu-manager', controller_file, '--wsapi-port %i' % self.RYU_REST_PORT])

        # install default forwarding rules
        # - duplicate to first filter table
        # - forward on correct output port (as we don't have a real destination in this test, it is omitted
        # - whatever doesn't match a filter is forwarded to the next
        fm_builder = FlowModMsgBuilder()
        fm_builder.add_flow_mod('insert', 'forwarding', 5, {}, {'fwd': ['filter_1']})
        for i in range(1, 100):
            fm_builder.add_flow_mod('insert', 'filter_%i' % i, 1, {}, {'fwd': ['filter_%i' % (i + 1, )]})

        self.push_flow_rules(fm_builder.get_msg())

    def push_flow_rules(self, msg):
        payload = msg
        r = requests.post(self.url, data=json.dumps(payload))

        if r.status_code == requests.codes.ok:
            self.logger.debug("FlowMod Succeeded - %s" % (str(r.status_code), ))
        else:
            self.logger.error("FlowMod Failed - %s" % (str(r.status_code), ))

    def create_interfaces(self):
        self.logger.info('create interfaces')
        for key in self.interfaces.keys():
            inter = Interfaces(self.interfaces[key][0], self.interfaces[key][1])
            inter.setup()

    def get_emitter_port(self):
        return self.EMITTER_PORT


class OFPTopo(Topo):
    def __init__(self, **opts):
        # Initialize topology and default options
        Topo.__init__(self, **opts)
        switch = self.addSwitch('s1')
