"""
### Sort Operation

Sorting corresponds to the `ORDER BY` part of an SQL query.

The UI would normally require the records to be sorted by some field, or fields.

The sort operation lets the API user specify the sorting of the results,
which makes sense for API endpoints that return a list of items.

An example of a sort operation would look like this:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    // sort by age, descending;
    // then sort by first name, alphabetically
    sort: ['age-', 'first_name+'],
}))
```

#### Syntax

* Array syntax.

    List of column names, optionally suffixed by the sort direction: `-` for `DESC`, `+` for `ASC`.
    The default is `+`.

    Example:

    ```javascript
    { sort: [ 'a+', 'b-', 'c' ] }  // -> a ASC, b DESC, c DESC
    ```

* String syntax

    List of columns, with optional `+` / `-`, separated by whitespace.

    Example:

    ```javascript
    { sort: 'a+ b- c' }
    ```

Object syntax is not supported because it does not preserve the ordering of keys.
"""

from collections import OrderedDict

from .base import MongoQueryHandlerBase
from ..bag import CombinedBag, FakeBag
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

    def __init__(self, model, bags, legacy_fields=None):
        # Legacy fields
        self.legacy_fields = frozenset(legacy_fields or ())

        # Parent
        super(MongoSort, self).__init__(model, bags)

        # On input
        #: OderedDict() of a sort spec: {key: +1|-1}
        self.sort_spec = None

    def _get_supported_bags(self):
        return CombinedBag(
            col=self.bags.columns,
            hybrid=self.bags.hybrid_properties,
            assocproxy=self.bags.association_proxies,
            legacy=FakeBag({n: None for n in self.legacy_fields}),
        )

    def _input(self, spec):
        """ Reusable method: fits both MongoSort and MongoGroup """

        # Empty
        if not spec:
            spec = []

        # String syntax
        if isinstance(spec, str):
            # Split by whitespace and convert to a list
            spec = spec.split()

        # List
        if isinstance(spec, (list, tuple)):
            # Strings: convert "column[+-]" into an ordered dict
            if all(isinstance(v, str) for v in spec):
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
            raise InvalidQueryError('{name} must be either a list, a string, or an object; {type} provided.'
                                    .format(name=self.query_object_section_name, type=type(spec)))

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
            if name not in self.supported_bags.bag('legacy')  # remove fake items
        ]

    # Not Implemented for this Query Object handler
    compile_options = NotImplemented
    compile_statement = NotImplemented
    compile_statements = NotImplemented

    def alter_query(self, query, as_relation=None):
        if not self.sort_spec:
            return query  # short-circuit
        return query.order_by(*self.compile_columns())

    def get_final_input_value(self):
        return [f'{name}{"-" if d == -1 else ""}'
                for name, d in self.sort_spec.items()]

    # Extra stuff

    def undefer_columns_involved_in_sorting(self, as_relation):
        """ undefer() columns required for this sort """
        # Get the names of the columns
        order_by_column_names = [c.key or c.element.key
                                 for c in self.compile_columns()]

        # Return options: undefer() every column
        return (as_relation.undefer(column_name)
                for column_name in order_by_column_names)
