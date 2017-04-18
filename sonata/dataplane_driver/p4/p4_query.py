#!/usr/bin/env python
# Author: Ruediger Birkner (Networked Systems Group at ETH Zurich)


from p4_elements import Action, Header, Table
from p4_operators import P4Distinct, P4Filter, P4Map, P4MapInit, P4Reduce
from p4_primitives import ModifyField, AddHeader
from sonata.dataplane_driver.utils import get_logger

# TODO Need to distinguish TCP and UDP ports!
HEADER_MAP = {'sIP': 'ipv4.srcAddr', 'dIP': 'ipv4.dstAddr',
              'sPort': 'tcp.srcPort', 'dPort': 'tcp.dstPort',
              'nBytes': 'ipv4.totalLen', 'proto': 'ipv4.protocol',
              'sMac': 'ethernet.srcAddr', 'dMac': 'ethernet.dstAddr', 'payload': ''}

HEADER_SIZE = {'sIP': 32, 'dIP': 32, 'sPort': 16, 'dPort': 16,
               'nBytes': 16, 'proto': 16, 'sMac': 48, 'dMac': 48,
               'qid': 16, 'count': 16}


# Class that holds one refined query - which consists of an ordered list of operators
class P4Query(object):
    def __init__(self, query_id, parse_payload, generic_operators, nop_name, drop_meta_field, satisfied_meta_field,
                 clone_meta_field):

        # LOGGING
        self.logger = get_logger('P4Query - %i' % query_id, 'INFO')
        self.logger.info('init')

        self.id = query_id
        self.parse_payload = parse_payload
        self.meta_init_name = ''

        self.src_to_filter_operator = dict()

        self.nop_action = nop_name

        self.drop_meta_field = '%s_%i' % (drop_meta_field, self.id)
        self.satisfied_meta_field = '%s_%i' % (satisfied_meta_field, self.id)
        self.clone_meta_field = clone_meta_field

        # general drop action which is applied when a packet doesn't satisfy this query
        self.actions = dict()
        self.actions['drop'] = Action('drop_%i' % self.id, (ModifyField(self.drop_meta_field, 1)))
        self.query_drop_action = self.actions['drop'].get_name()

        # action and table to mark query as satisfied at end of query processing in ingress
        primitives = list()
        primitives.append(ModifyField(self.satisfied_meta_field, 1))
        primitives.append(ModifyField(self.clone_meta_field, 1))
        self.actions['satisfied'] = Action('do_mark_satisfied_%i' % self.id, primitives)
        self.satisfied_table = Table('mark_satisfied_%i' % self.id, self.actions['satisfied'].get_name(), [], None, 1)

        # initialize operators
        self.operators = self.init_operators(generic_operators)

        # out_header
        fields = ['qid'] + self.operators[-1].get_out_headers()
        self.out_header_fields = [(field, HEADER_SIZE[field]) for field in fields]
        self.out_header = Header('out_header_%i' % self.id, self.out_header_fields)

        # action and table to populate out_header in egress
        primitives = list()
        primitives.append(AddHeader(self.out_header.get_name()))
        for field_name, _ in self.out_header_fields:
            primitives.append(ModifyField('%s.%s' % (self.out_header.get_name(), field_name),
                                          '%s.%s' % (self.meta_init_name, field_name)))
        self.actions['add_out_header'] = Action('do_add_out_header_%i' % self.id, primitives)

        self.out_header_table = Table('add_out_header_%i' % self.id, self.actions['add_out_header'].get_name(), [],
                                      None, 1)

    def init_operators(self, generic_operators):
        p4_operators = list()
        operator_id = 1

        # Add map init
        keys = set()
        keys.add('qid')
        for operator in generic_operators:
            if operator.name in {'Filter', 'Map', 'Reduce', 'Distinct'}:
                keys = keys.union(set(operator.get_init_keys()))
        # TODO remove this
        keys = filter(lambda x: x != 'payload' and x != 'ts', keys)

        self.logger.debug('add map_init with keys: %s' % (', '.join(keys),))
        map_init_operator = P4MapInit(self.id, operator_id, keys)
        self.meta_init_name = map_init_operator.get_meta_name()
        p4_operators.append(map_init_operator)

        # add all the other operators one after the other
        for operator in generic_operators:
            self.logger.debug('add %s operator' % (operator.name,))
            operator_id += 1
            if operator.name == 'Filter':
                match_action = self.nop_action
                miss_action = self.query_drop_action
                filter_operator = P4Filter(self.id,
                                           operator_id,
                                           operator.keys,
                                           operator.filter_keys,
                                           operator.func,
                                           operator.src,
                                           match_action,
                                           miss_action)
                if operator.src != 0:
                    self.src_to_filter_operator[operator.src] = filter_operator
                p4_operators.append(filter_operator)

            elif operator.name == 'Map':
                p4_operators.append(P4Map(self.id,
                                          operator_id,
                                          self.meta_init_name,
                                          operator.keys,
                                          operator.map_keys,
                                          operator.func))

            elif operator.name == 'Reduce':
                p4_operators.append(P4Reduce(self.id,
                                             operator_id,
                                             self.meta_init_name,
                                             self.query_drop_action,
                                             operator.keys,
                                             operator.threshold))

            elif operator.name == 'Distinct':
                p4_operators.append(P4Distinct(self.id,
                                               operator_id,
                                               self.meta_init_name,
                                               self.query_drop_action,
                                               self.nop_action,
                                               operator.keys,))

            else:
                self.logger.error('tried to add an unsupported operator: %s' % operator.name)
        return p4_operators

    def get_ingress_control_flow(self, indent_level):
        curr_indent_level = indent_level

        indent = '\t' * curr_indent_level
        out = '%s// query %i\n' % (indent, self.id)
        # apply one operator after another
        for operator in self.operators:
            indent = '\t' * curr_indent_level
            curr_indent_level += 1
            out += '%sif (%s != 1) {\n' % (indent, self.drop_meta_field)
            out += operator.get_control_flow(curr_indent_level)

        # mark packet as satisfied if it has never been marked as dropped
        indent = '\t' * curr_indent_level
        out += '%sif (%s != 1) {\n' % (indent, self.drop_meta_field)
        out += '%s\tapply(%s);\n' % (indent, self.satisfied_table.get_name())
        out += '%s}\n' % indent

        # close brackets
        for _ in self.operators:
            curr_indent_level -= 1
            indent = '\t' * curr_indent_level
            out += '%s}\n' % indent

        return out

    def get_egress_control_flow(self, indent_level):
        indent = '\t' * indent_level

        out = '%sif (%s == 1) {\n' % (indent, self.satisfied_meta_field)
        out += '%s\tapply(%s);\n' % (indent, self.out_header_table.get_name())
        out += '%s}\n' % indent
        return out

    def get_code(self):
        out = '// query %i\n' % self.id

        # out header
        out += self.out_header.get_code()

        # query actions (drop, mark satisfied, add out header, etc)
        for action in self.actions.values():
            out += action.get_code()

        # query tables (add out header, mark satisfied)
        out += self.out_header_table.get_code()
        out += self.satisfied_table.get_code()

        # operator code
        for operator in self.operators:
            out += operator.get_code()
        return out

    def get_commands(self):
        commands = list()
        for operator in self.operators:
            print str(operator)
            commands.extend(operator.get_commands())

        commands.append(self.out_header_table.get_default_command())
        commands.append(self.satisfied_table.get_default_command())

        return commands

    def get_out_header(self):
        return self.out_header.get_name()

    def get_metadata_name(self):
        return self.meta_init_name

    def get_header_format(self):
        header_format = dict()
        header_format['parse_payload'] = self.parse_payload
        header_format['headers'] = self.out_header_fields
        return header_format

    def get_update_commands(self, filter_id, update):
        commands = list()
        if filter_id in self.src_to_filter_operator:
            filter_operator = self.src_to_filter_operator[filter_id]
            filter_mask = filter_operator.get_filter_mask()
            filter_table_name = filter_operator.table.get_name()
            filter_action = filter_operator.get_match_action()

            for dip in update:
                dip = dip.strip('\n')
                commands.append('table_add %s %s  %s/%i =>' % (filter_table_name, filter_action, dip, filter_mask))
        return commands
