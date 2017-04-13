#!/usr/bin/env python


class OFPTarget(object):
    def __init__(self):
        self.supported_operations = ['Filter']

    def get_supported_operators(self):
        return self.supported_operations

    def run(self, app):
        pass

    def update(self, filter_update):
        pass
