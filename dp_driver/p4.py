#!/usr/bin/env python


class P4Target(object):
    def __init__(self):
        pass


# TODO integrate p4 queries here

class P4Operator(object):
    def __init__(self, name):
        self.name = name
        self.tables = list()
        self.registers = list()
        self.metadata = list()


class P4Reduce(P4Operator):
    def __init__(self):
        super(P4Reduce, self).__init__('Reduce')


class P4Map(P4Operator):
    def __init__(self):
        super(P4Map, self).__init__('Map')


class P4Distinct(P4Operator):
    def __init__(self):
        super(P4Distinct, self).__init__('Distinct')


class P4Filter(P4Operator):
    def __init__(self):
        super(P4Filter, self).__init__('Filter')


class P4Truncate(P4Operator):
    def __init__(self):
        super(P4Truncate, self).__init__('Truncate')


# TODO integrate p4 queries here

class P4Element(object):
    def __init__(self, name):
        self.name = name


class Register(P4Element):
    def __init__(self):
        super(Register, self).__init__('Register')


class Table(P4Element):
    def __init__(self):
        super(Table, self).__init__('Table')


class MetaData(P4Element):
    def __init__(self):
        super(MetaData, self).__init__('MetaData')
