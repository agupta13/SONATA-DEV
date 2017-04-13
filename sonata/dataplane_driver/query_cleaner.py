#!/usr/bin/env python

from sonata.query_engine.sonata_operators.map import Map
from sonata.query_engine.sonata_operators.join import Join
from sonata.query_engine.sonata_operators.filter import Filter
from sonata.query_engine.sonata_operators.reduce import Reduce
from sonata.query_engine.sonata_operators.distinct import Distinct

from query_object import QueryObject

from utils import get_logger


def get_clean_application(application):
    # LOGGING
    logger = get_logger('CleanApplication', 'INFO')
    new_app = dict()
    for query_id, query in application.iteritems():
        new_qo = QueryObject(query_id)
        new_qo.parse_payload = query.parse_payload
        skip_next_filter = False
        for index, operator in enumerate(query.operators):
            # skip filter following a reduce as the reduce and filter are combined:
            if skip_next_filter and operator.name == 'Filter':
                skip_next_filter = False
                continue
            else:
                skip_next_filter = False
            new_o = None
            keys = filter(lambda x: x != 'ts', operator.keys)

            if operator.name == 'Map':
                new_o = Map()
                # drop Map operators without a supported function
                if len(operator.func) > 0:
                    if isinstance(operator.map_keys, tuple):
                        new_o.map_keys = [operator.map_keys[0]]
                    else:
                        new_o.map_keys = operator.map_keys
                    new_o.keys = keys
                    new_o.map_values = operator.map_values
                    new_o.values = operator.values
                    new_o.prev_keys = operator.prev_keys
                    new_o.prev_values = operator.prev_values
                    new_o.func = operator.func
                    new_qo.operators.append(new_o)
            elif operator.name == 'Distinct':
                new_o = Distinct()
                new_o.keys = keys
                new_o.values = operator.values
                new_o.prev_keys = operator.prev_keys
                new_o.prev_values = operator.prev_values
                new_qo.operators.append(new_o)
            elif operator.name == 'Filter':
                new_o = Filter()
                new_o.keys = keys
                new_o.values = operator.values
                new_o.prev_keys = operator.prev_keys
                new_o.prev_values = operator.prev_values
                new_o.filter_keys = operator.filter_keys
                new_o.filter_vals = operator.filter_vals
                new_o.func = tuple(operator.func)
                new_o.src = operator.src
            elif operator.name == 'Join':
                new_o = Join()
                new_o.query = operator.query
                new_qo.operators.append(new_o)
            elif operator.name == 'Reduce':
                new_o = Reduce()
                new_o.keys = keys
                new_o.values = operator.values
                new_o.prev_keys = operator.prev_keys
                new_o.prev_values = operator.prev_values
                new_o.func = operator.func
                next_operator = query.operators[index + 1]

                # merge Reduce and following Filter if the filter is on count and uses geq as function
                if next_operator.name == 'Filter' and 'count' in next_operator.filter_values and next_operator.func[0] == 'geq':
                    skip_next_filter = True
                    filter_value = next_operator.func[1]
                    new_o.threshold = filter_value
                else:
                    logger.error('reduce operator without a following, valid filter')
                new_qo.operators.append(new_o)
            else:
                print "Found a unsupported operator: %s" % (operator.name, )
        new_app[new_qo.id] = new_qo

    return new_app