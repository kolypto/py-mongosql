from __future__ import absolute_import
from future.utils import string_types

from collections import OrderedDict

from .base import MongoQueryHandlerBase
from ..bag import CombinedBag
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class MongoSort(MongoQueryHandlerBase):
    """ MongoDB sorting

        * None: no sorting
        * OrderedDict({ a: +1, b: -1 })
        * [ 'a+', 'b-', 'c' ]  - array of strings '<column>[<+|->]'. default direction = +1
        * dict({a: +1}) -- you can only use a dict with ONE COLUMN (because of its unstable order)

        Supports: Columns, hybrid properties
    """

    query_object_section_name = 'sort'

    def __init__(self, model, bags):
        super(MongoSort, self).__init__(model, bags)

        # On input
        #: OderedDict() of a sort spec: {key: +1|-1}
        self.sort_spec = None

    def _get_supported_bags(self):
        return CombinedBag(
            col=self.bags.columns,
            hybrid=self.bags.hybrid_properties
        )

    def _input(self, spec):
        """ Reusable method: fits both MongoSort and MongoGroup """

        # Empty
        if not spec:
            spec = []

        # List
        if isinstance(spec, (list, tuple)):
            # Strings: convert "column[+-]" into an ordered dict
            if all(isinstance(v, string_types) for v in spec):
                spec = OrderedDict([
                    [v[:-1], -1 if v[-1] == '-' else +1]
                    if v[-1] in {'+', '-'}
                    else [v, +1]
                    for v in spec
                ])

        # Dict
        if isinstance(spec, OrderedDict):
            pass  # nothing to do here
        elif isinstance(spec, dict):
            if len(spec) > 1:
                raise InvalidQueryError('{} is a plain object; can only have 1 column '
                                        'because of unstable ordering of object keys; '
                                        'use list syntax instead'
                                        .format(self.query_object_section_name))
            spec = OrderedDict(spec)
        else:
            raise InvalidQueryError('{} must be either an object or a list'
                                    .format(self.query_object_section_name))

        # Validate directions: +1 or -1
        if not all(dir in {-1, +1} for field, dir in spec.items()):
            raise InvalidQueryError('{} direction can be either +1 or -1'.format(self.query_object_section_name))

        # Validate columns
        self.validate_properties(spec.keys())
        return spec

    def input(self, sort_spec):
        super(MongoSort, self).input(sort_spec)
        self.sort_spec = self._input(sort_spec)
        return self

    def merge(self, sort_spec):
        self.sort_spec.update(self._input(sort_spec))
        return self

    def compile_columns(self):
        return [
            self.supported_bags.get(name).desc() if d == -1 else self.supported_bags.get(name)
            for name, d in self.sort_spec.items()
        ]

    # Not Implemented for this Query Object handler
    compile_options = NotImplemented
    compile_statement = NotImplemented
    compile_statements = NotImplemented

    def alter_query(self, query, as_relation=None):
        if not self.sort_spec:
            return query  # short-circuit
        return query.order_by(*self.compile_columns())

    # Extra stuff

    def undefer_columns_involved_in_sorting(self, as_relation):
        """ undefer() columns required for this sort """
        # Get the names of the columns
        order_by_column_names = [c.key or c.element.key
                                 for c in self.compile_columns()]

        # Return options: undefer() every column
        return (as_relation.undefer(column_name)
                for column_name in order_by_column_names)
