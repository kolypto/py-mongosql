from collections import OrderedDict

from sqlalchemy import Integer
from sqlalchemy.orm import load_only, defaultload, lazyload, contains_eager, aliased
from sqlalchemy.sql.expression import and_, or_, not_, cast
from sqlalchemy.sql import operators
from sqlalchemy.sql.functions import func

from sqlalchemy.dialects import postgresql as pg


class _MongoStatement(object):
    def __call__(self, model):
        raise NotImplementedError()


class MongoProjection(_MongoStatement):
    """ MongoDB projection operator

        * { a: 1, b: 1 } - include only the given fields
        * { a: 0, b: 0 } - exlude the given fields
        * [ a, b, c ] - include only the given fields
    """

    def __init__(self, projection):
        """ Create a projection

            :type projection: None | str | list | tuple | dict
            :raises AssertionError: invalid input
        """

        #: Inclusion mode or exclusion mode?
        self.inclusion_mode = False

        #: Normalized projection: { field_name: 0|1 }
        self.projection = {}

        # Empty projection
        if not projection:
            self.inclusion_mode = False
            self.projection = {}
            return

        # Array syntax
        if isinstance(projection, (list, tuple)):
            self.inclusion_mode = True
            self.projection = {k: 1 for k in projection}
            return

        # Object syntax
        assert isinstance(projection, dict), 'Projection must be one of: None, list, dict'
        assert sum(projection.values()) in [0, len(projection)], 'Dict projection values shall be all 0s or all 1s'

        self.projection = projection
        self.inclusion_mode = any(projection.values())

    @classmethod
    def columns(cls, bag, projection, inclusion_mode):
        """ Get the list of columns to be included

            :type bag: mongosql.bag.ModelPropertyBags
            :rtype: sqlalchemy.orm.Load
            :return: Options to include columns
            :raises AssertionError: unknown column name
        """
        if inclusion_mode:
            # Columns
            return (bag.columns[name] for name, inc in projection.items())
        else:
            # Check columns
            projection_keys = set(projection.keys())
            assert projection_keys <= bag.columns.names, 'Invalid column specified in projection'
            # Columns
            return (col for name, col in bag.columns.items() if name not in projection_keys)

    @classmethod
    def options(cls, bag, projection, inclusion_mode):
        """ Get query options for the columns """
        return map(load_only,
                   cls.columns(bag, projection, inclusion_mode))

    def __call__(self, model):
        """ Build the statement

            :type model: MongoModel
            :rtype: list[sqlalchemy.sql.schema.Column]
            :return: The list of columns to include
            :raises AssertionError: unknown column name
        """
        return self.options(model.model_bag, self.projection, self.inclusion_mode)


class MongoSort(_MongoStatement):
    """ MongoDB sorting

        * OrderedDict({ a: +1, b: -1 })
        * [ 'a+', 'b-', 'c' ]  - array of strings '<column>[<+|->]'. default direction = +1
    """

    def __init__(self, sort_spec):
        """ Create the sorter

            :type sort_spec: string | list | tuple | OrderedDict
        """

        #: Normalized sort: { field: +1 | -1 }
        self.sort = OrderedDict()

        # Empty
        if not sort_spec:
            sort_spec = []

        # List
        if isinstance(sort_spec, (list, tuple)):
            # Strings
            if all(isinstance(v, basestring) for v in sort_spec):
                sort_spec = OrderedDict([
                    [v[:-1], -1 if v[-1] == '-' else +1]
                    if v[-1] in {'+', '-'}
                    else [v, +1]
                    for v in sort_spec
                ])

        # OrderedDict
        if isinstance(sort_spec, OrderedDict):
            self.sort = sort_spec

            # Check directions
            assert all(dir in {-1, +1} for field, dir in self.sort.items()), '{} direction can be either +1 or -1'.format(type(self).__name__)
            return

        # Otherwise
        raise AssertionError('{} must be one of: None, list, OrderedDict'.format(type(self).__name__))

    @classmethod
    def columns(cls, bag, sort):
        """ Get the list of sorters for columns

            :type bag: mongosql.bag.ModelPropertyBags
            :rtype: list
            :return: The list of sort specifications

            :raises AssertionError: unknown column name
        """
        return [
            bag.columns[name].desc() if d == -1 else bag.columns[name]
            for name, d in sort.items()
        ]

    def __call__(self, model):
        """ Build the statement

            :type model: MongoModel
            :rtype: list
            :return: Sort columns

            :raises AssertionError: unknown column name
        """
        return self.columns(model.model_bag, self.sort)


class MongoGroup(MongoSort):
    """ MongoDB-style grouping

        See :cls:MongoSort
    """

    def __init__(self, group_spec):
        """ Create the grouper

            :type group_spec: string | list | tuple | OrderedDict
        """
        super(MongoGroup, self).__init__(group_spec)

    def __call__(self, model):
        """ Build the statement

            :type model: MongoModel
            :rtype: list
            :return: The list of column groupers

            :raises AssertionError: unknown column name
        """
        return self.columns(model.model_bag, self.sort)


class MongoCriteria(_MongoStatement):
    """ MongoDB criteria

        Supports the following MongoDB operators:

        * { a: 1 }  - equality. For arrays: contains value.
        * { a: { $lt: 1 } }  - <
        * { a: { $lte: 1 } } - <=
        * { a: { $ne: 1 } } - <>. For arrays: does not contain value
        * { a: { $gte: 1 } } - >=
        * { a: { $gt: 1 } } - >
        * { a: { $in: [...] } } - any of. For arrays: has any from
        * { a: { $nin: [...] } } - none of. For arrays: has none from

        * { a: { $exists: true } } - is [not] NULL

        * { arr: { $all: [...] } } For arrays: contains all values
        * { arr: { $size: 0 } } For arrays: has a length of 0

        Supports the following boolean operators:

        * { $or: [ {..criteria..}, .. ] }  - any is true
        * { $and: [ {..criteria..}, .. ] } - all are true
        * { $nor: [ {..criteria..}, .. ] } - none is true
        * { $not: { ..criteria.. } } - negation
    """

    def __init__(self, criteria):
        if not criteria:
            criteria = {}
        assert isinstance(criteria, dict), 'Criteria must be one of: None, dict'
        self.criteria = criteria

    # noinspection PyComparisonWithNone
    @classmethod
    def statement(cls, bag, criteria):
        """ Create a statement from criteria
        :type bag: mongosql.bag.ModelPropertyBags
        :rtype: sqlalchemy.sql.elements.BooleanClauseList
        """
        # Assuming a dict of { column: value } and { column: { $op: value } }

        conditions = []
        for col_name, criteria in criteria.items():
            # Boolean expressions?
            if col_name in {'$or', '$and', '$nor'}:
                assert isinstance(criteria, (list, tuple)), 'Criteria: {} argument must be a list'.format(col_name)
                if len(criteria) == 0:
                    continue  # skip empty

                criteria = map(lambda s: cls.statement(bag, s), criteria)  # now a list of expressions
                if col_name == '$or':
                    cc = or_(*criteria)
                elif col_name == '$and':
                    cc = and_(*criteria)
                elif col_name == '$nor':
                    cc = or_(*criteria)
                    conditions.append(~cc.self_group() if len(criteria) > 1 else ~cc)
                    continue
                else:
                    raise KeyError('Unknown operator '+col_name)

                conditions.append(cc.self_group() if len(criteria) > 1 else cc)
                continue
            elif col_name == '$not':
                assert isinstance(criteria, dict), 'Criteria: $not argument must be a dict'
                criteria = cls.statement(bag, criteria)
                conditions.append(not_(criteria))
                continue

            # Prepare
            col = bag.columns[col_name]
            col_array = bag.columns.is_column_array(col_name)
            col_json  = bag.columns.is_column_json(col_name)

            # Fake equality
            if not isinstance(criteria, dict):
                criteria = {'$eq': criteria}  # fake the missing equality operator for simplicity

            # Iterate over operators
            for op, value in criteria.items():
                value_array = isinstance(value, (list, tuple))

                # Coerce operand
                if col_array and value_array:
                    value = cast(pg.array(value), pg.ARRAY(col.type.item_type))
                if col_json:
                    coerce_type = col.type.coerce_compared_value('=', value)  # HACKY: use sqlalchemy type coercion
                    col = cast(col.astext, coerce_type)

                # Operators
                if op == '$eq':
                    if col_array:
                        if value_array:
                            conditions.append(col == value)  # Array equality
                        else:
                            conditions.append(col.any(value))  # ANY(array) = value, for scalar values
                    else:
                        conditions.append(col == value)  # array == value, for both scalars
                elif op == '$ne':
                    if col_array and not value_array:
                        if value_array:
                            conditions.append(col != value)  # Array inequality
                        else:
                            conditions.append(col.all(value, operators.ne))  # ALL(array) != value, for scalar values
                    else:
                        conditions.append(col != value)  # array != value, for both scalars
                elif op == '$lt':
                    conditions.append(col < value)
                elif op == '$lte':
                    conditions.append(col <= value)
                elif op == '$gte':
                    conditions.append(col >= value)
                elif op == '$gt':
                    conditions.append(col > value)
                elif op == '$in':
                    assert value_array, 'Criteria: $in argument must be a list'
                    if col_array:
                        conditions.append(col.overlap(value))  # field && ARRAY[values]
                    else:
                        conditions.append(col.in_(value))  # field IN(values)
                elif op == '$nin':
                    assert value_array, 'Criteria: $nin argument must be a list'
                    if col_array:
                        conditions.append(~ col.overlap(value))  # NOT( field && ARRAY[values] )
                    else:
                        conditions.append(col.notin_(value))  # field NOT IN(values)
                elif op == '$exists':
                    if value:
                        conditions.append(col != None)  # IS NOT NULL
                    else:
                        conditions.append(col == None)  # IS NULL
                elif op == '$all':
                    assert col_array, 'Criteria: $all can only be applied to an array column'
                    assert value_array, 'Criteria: $all argument must be a list'
                    conditions.append(col.contains(value))
                elif op == '$size':
                    assert col_array, 'Criteria: $all can only be applied to an array column'
                    if value == 0:
                        conditions.append(func.array_length(col, 1) == None)  # ARRAY_LENGTH(field, 1) IS NULL
                    else:
                        conditions.append(func.array_length(col, 1) == value)  # ARRAY_LENGTH(field, 1) == value
                else:
                    raise AssertionError('Criteria: unsupported operator "{}"'.format(op))
        if conditions:
            cc = and_(*conditions)
            return cc.self_group() if len(conditions) > 1 else cc
        else:
            return True

    def __call__(self, model):
        """ Build the statement

            :type model: MongoModel
            :return: SQL statement for filter()
            :rtype: sqlalchemy.sql.elements.BooleanClauseList
            :raises AssertionError: unknown column name
        """
        return self.statement(model.model_bag, self.criteria)


class _MongoJoinParams(object):
    def __init__(self, options, relationship=None, target_model=None, query=None):
        """ Values for joins
        :param options: Additional query options
        :type options: list[sqlalchemy.orm.Load]
        :param relationship: Relationship
        :type relationship: sqlalchemy.orm.attributes.InstrumentedAttribute
        :param target_model: Target model
        :type target_model: sqlalchemy.ext.declarative.api.DeclarativeMeta
        :param query: Query dict for :meth:MongoQuery.query()
        :type query: dict
        """
        self.options = options
        self.relationship = relationship
        self.target_model = target_model
        self.query = query


class MongoJoin(_MongoStatement):
    """ Joining relations (eager load)

        - List of relation names
        - Dict: { relation-name: query-dict } for :meth:MongoQuery.query
    """

    def __init__(self, relnames, as_relation=None):
        """ Create the joiner

        :param relnames: List of relation names to load eagerly
        :type relnames: list[str]
        :param as_relation: Base relation to chain the loader options from
        :type as_relation: sqlalchemy.orm.relationships.RelationshipProperty
        """

        if not relnames:
            self.rels = {}
        elif isinstance(relnames, (list, tuple)):
            self.rels = {relname: None for relname in relnames}
        elif isinstance(relnames, dict):
            self.rels = relnames
        else:
            raise AssertionError('Join must be one of: None, list, tuple, dict')

        self.as_relation = as_relation

    @classmethod
    def options(cls, bag, rels, as_relation):
        """ Prepare relationships loader
        :type bag: mongosql.bag.ModelPropertyBags
        :returns: List of _MongoJoinParams
        :rtype: list[_MongoJoinParams]
        """
        relnames = set(rels.keys())
        assert relnames <= bag.relations.names, 'Invalid relation names: {}'.format(relnames - bag.relations.names)

        # Loader options:
        lbase = defaultload(as_relation) if as_relation else None
        _lazyload       = lambda rel: lbase.lazyload(rel) if lbase else lazyload(rel)
        _contains_eager = lambda rel: lbase.contains_eager(rel) if lbase else contains_eager(rel)

        # Complex joins
        mjp_list = []
        for relname, query in rels.items():
            if query is None:
                pass
            else:
                rel = bag.relations[relname]
                rel_a = aliased(rel)
                target_model = rel.property.mapper.class_

                mjp_list.append(_MongoJoinParams(
                    [_contains_eager(rel)],
                    rel,
                    target_model,
                    query
                ))

        # lazyload() on all other relations
        opts = [_lazyload(bag.relations[relname]) for relname in bag.relations.names if relname not in relnames]  # FIXME: apply lazyload() to all attributes initially, then override these. How do I do it?  http://stackoverflow.com/questions/25000473/
        mjp_list.append(_MongoJoinParams(opts))

        # Finish
        return mjp_list

    def __call__(self, model):
        """ Build the statement

            :type model: MongoModel
            :return: List of join params
            :rtype: list[_MongoJoinParams]
            :raises AssertionError: unknown column name
        """
        return self.options(model.model_bag, self.rels, self.as_relation)


class MongoAggregate(_MongoStatement):
    """ Aggregation statements

        { computed_field_name: aggregation-expression }

        Aggregation expressions:

            * column-name
            * { $min: operand }
            * { $max: operand }
            * { $avg: operand }
            * { $sum: operand }

        An operand can be:

            - Integer (for counting): { $sum: 1 }
            - Column name
            - Boolean expression (see :cls:MongoCriteria)
    """

    def __init__(self, agg_spec):
        """ Create aggregation
        :param agg_spec: Aggregation spec
        :type agg_spec: dict
        """
        if not agg_spec:
            agg_spec = {}
        assert isinstance(agg_spec, dict), 'Aggregate spec must be one of: None, dict'
        self.agg_spec = agg_spec

    @classmethod
    def selectables(cls, bag, agg_spec):
        """ Create a list of statements from spec
        :type bag: mongosql.bag.ModelPropertyBags
        :rtype: list[sqlalchemy.sql.elements.ColumnElement]
        """
        # TODO: calculation expressions for selection: http://docs.mongodb.org/manual/meta/aggregation-quick-reference/
        selectables = []
        for comp_field, comp_expression in agg_spec.items():
            # Column reference
            if isinstance(comp_expression, basestring):
                selectables.append(bag.columns[comp_expression].label(comp_field))
                continue

            # Computed expression
            assert isinstance(comp_expression, dict), 'Aggregate: Expression should be either a column name, or an object'
            assert len(comp_expression) == 1, 'Aggregate: expression can only contain a single operator'
            operator, expression = comp_expression.popitem()

            # Expression statement
            if isinstance(expression, int) and operator == '$sum':
                # Special case for count
                expression_stmt = expression
            elif isinstance(expression, basestring):
                # Column name
                expression_stmt = bag.columns[expression]
            elif isinstance(expression, dict):
                # Boolean expression
                expression_stmt = MongoCriteria.statement(bag, expression)
                # Need to cast it to int
                expression_stmt = cast(expression_stmt, Integer)
            else:
                raise AssertionError('Aggregate: expression should be either a column name, or an object')

            # Operator
            if operator == '$max':
                comp_stmt = func.max(expression_stmt)
            elif operator == '$min':
                comp_stmt = func.min(expression_stmt)
            elif operator == '$avg':
                comp_stmt = func.avg(expression_stmt)
            elif operator == '$sum':
                if isinstance(expression_stmt, int):
                    # Special case for count
                    comp_stmt = func.count()
                    if expression_stmt != 1:
                        comp_stmt *= expression_stmt
                else:
                    comp_stmt = func.sum(expression_stmt)
            else:
                raise AssertionError('Aggregate: unsupported operator "{}"'.format(operator))

            # Append
            selectables.append(comp_stmt.label(comp_field))

        return selectables

    def __call__(self, model):
        """ Build the statement

            :type model: MongoModel
            :return: List of selectables
            :rtype: list[sqlalchemy.sql.elements.ColumnElement]
            :raises AssertionError: wrong expression
        """
        return self.selectables(model.model_bag, self.agg_spec)

# TODO: update operations in MongoDB-style
