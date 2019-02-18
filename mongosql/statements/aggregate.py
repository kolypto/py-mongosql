from __future__ import absolute_import
from future.utils import string_types

from sqlalchemy import Integer, Float

from sqlalchemy.sql.expression import cast
from sqlalchemy.sql.functions import func

from .base import _MongoQueryStatementBase
from .reusable import Reusable
from ..bag import CombinedBag
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class AggregateExpressionBase(object):
    """ Represents a computed field with a label """
    def __init__(self, label):
        self.label = label

    def labeled_expression(self, expr):
        return expr.label(self.label)

    def compile(self):
        """ Compile this aggregate expression into SqlAlchemy """
        raise NotImplementedError()


class AggregateLabelledColumn(AggregateExpressionBase):
    """ Represents a labeled column

        This operation just gives another name to a column

        The following case is handled here:
        { labeled_column: 'age' }
    """
    def __init__(self, label, column_name, column):
        super(AggregateLabelledColumn, self).__init__(label)
        self.column_name = column_name
        self.column = column

    def __repr__(self):
        return '{} -> {}'.format(self.column_name, self.label)

    def compile(self):
        return self.labeled_expression(self.column)


class AggregateColumnOperator(AggregateExpressionBase):
    """ Represents an aggregation operator applied to a column

        The following case is handled here:
        { minimal_age: { $min: 'age' }}
        operator=$min, column_name='age', column=User.age, label='minimal_age'
    """
    def __init__(self, label, operator, column_name, column, is_column_json):
        super(AggregateColumnOperator, self).__init__(label)
        self.operator = operator
        self.column_name = column_name
        self.column = column
        self.is_column_json = is_column_json

    def __repr__(self):
        return '{} {}'.format(self.operator, self.column_name)

    def compile(self):
        # Json column?
        if self.is_column_json:
            # PostgreSQL always returns text values from it, and for aggregation we usually need numbers :)
            column = cast(self.column, Float)
        else:
            # Simply use
            column = self.column

        # Now, handle the operator, and apply it to the expression
        if self.operator == '$max':
            stmt = func.max(column)
        elif self.operator == '$min':
            stmt = func.min(column)
        elif self.operator == '$avg':
            stmt = func.avg(column)
        elif self.operator == '$sum':
            stmt = func.sum(column)
        else:
            raise AssertionError('Aggregate: unsupported operator "{}"'.format(self.operator))
        return self.labeled_expression(stmt)


class AggregateBooleanCount(AggregateExpressionBase):
    """ Represents an aggregation over a boolean expression: count the number of positives

        The following case is handled here:
        { count_ripe_age: { $sum: { age: { $gt: 18 } } }}
        operator=$sum, expression={ age: { $gt: 18 } }, label='count_ripe_age'
    """

    def __init__(self, label, expression):
        """ Init a count over a boolean expression

        :type expression: MongoFilter | int
        """
        super(AggregateBooleanCount, self).__init__(label)
        self.expression = expression

    def __repr__(self):
        return 'COUNT({})'.format(self.expression)

    def compile(self):
        # Remember that there is this special case: { $sum: 1 }
        if isinstance(self.expression, int):
            # Special case for count
            stmt = func.count()
            if self.expression != 1:
                # When $sum: N, we count N per row. That's multiplication
                stmt *= self.expression
        else:
            # Compile the boolean statement
            stmt = self.expression.compile_statement()
            # Sum the value of this expression (column, boolean, whatever)
            # Need to cast it to int
            stmt = cast(stmt, Integer)
            # Now, sum it
            stmt = func.sum(stmt)
        # Done
        return self.labeled_expression(stmt)



class MongoAggregate(_MongoQueryStatementBase):
    """ Aggregation statements

        You can choose a field name to be used, essentially, as a label, and assign an expression to it
        that's going to be computed.
        Syntax:

            { computed_field_name: aggregation-expression }

        WARNING: this can potentially expose sensitive data to an attacker!!!
            Your application should decide which columns it allows to be used in aggregation.
            See __init__() arguments that implement this security.

        Aggregation expressions that you can use:

            * column-name: essentially, give another name to a column.
                WARNING: this can potentially expose sensitive data to an attacker!!!
                It is disabled by default. See `enable_labels`
            * { $min: operand } - MIN on a numeric column
            * { $max: operand } - MAX
            * { $avg: operand } - AVG
            * { $sum: operand } - SUM. Can also be applied to `1` (to count columns),
                and to a boolean expression as an object

        An operand can be:

            - Integer (for counting): { $sum: 1 }
            - Column name
            - Boolean expression: MongoFilter query object syntax.
                In case a boolean expression is given to a $sum, it counts positives.
    """

    query_object_section_name = 'aggregate'

    def __init__(self, model, mongofilter, allowed_columns=(), enable_labels=False):
        """ Init aggregation

        :param model: Model
        :param mongofilter: A configured MongoFilter object to be used for boolean operators
        :type mongofilter: MongoFilter
        :param allowed_columns: list of columns for which aggregation is enabled
        :type allowed_columns: list[str]
        :param enable_labels: whether labelling columns is enabled
        :type enable_labels: bool
        """
        super(MongoAggregate, self).__init__(model)
        self.mongofilter = Reusable(mongofilter)

        # Security
        self.allowed_columns = set(allowed_columns)
        self.enable_labels = enable_labels

        # On input
        self.agg_spec = None

        # Validation
        self.validate_properties(self.allowed_columns, where='aggregate:allowed_columns')

    def _get_supported_bags(self):
        return CombinedBag(
            col=self.bags.columns,
            hybrid=self.bags.hybrid_properties,
        )

    def _get_column_insecurely(self, column_name, for_label=False):
        """ Get a column. Insecurely. Disrespect self.allowed_columns """
        try:
            bag_name, bag, column = self.supported_bags[column_name]
            return column
        except KeyError:
            raise InvalidColumnError(self.bags.model, column_name, 'aggregate')

    def _get_column_securely(self, column_name, for_label=False):
        """ Get a column. Securely. Respect self.allowed_columns """
        column = self._get_column_insecurely(column_name, for_label)
        if column_name not in self.allowed_columns:
            raise InvalidQueryError('Aggregate: aggregation is disabled for column `{}`'
                                    .format(column_name))
        if for_label and not self.enable_labels:
            raise InvalidQueryError('Aggregate: labelling is disabled for column `{}`'
                                    .format(column_name))
        return column

    def input(self, agg_spec):
        super(MongoAggregate, self).input(agg_spec)

        # Validate
        if not agg_spec:
            agg_spec = {}
        if not isinstance(agg_spec, dict):
            raise InvalidQueryError('aggregate: argument must be an object')

        # Transform the input into { label: int|Column|FilterBooleanExpression }
        self.agg_spec = self._parse_input(agg_spec)
        return self

    # These classes implement compilation
    # You can override them, if necessary
    _LABELLED_COLUMN_CLS = AggregateLabelledColumn
    _COLUMN_OPERATOR_CLS = AggregateColumnOperator
    _BOOLEAN_COUNT_CLS = AggregateBooleanCount

    def _parse_input(self, input):
        agg_spec = {}
        # `agg_spec` contains pairs of { operator: expression }. Iterate over it
        for comp_field_label, comp_expression in input.items():
            # The "expression" can be one of:
            # string: reference to a column, which simply receives a label
            # dict: computed expression, like the sum of items matching a certain criterion, like SUM(age > 18)
            # integer value: for this special case { $sum: 1 }
            # Here the logic forks depending on the type of the argument

            # string: Column reference
            if isinstance(comp_expression, string_types):
                column_name = comp_expression
                # get the column, give it a label
                column = self._get_column_securely(column_name, True)
                # add it to the output
                agg_spec[comp_field_label] = self._LABELLED_COLUMN_CLS(comp_field_label, column_name, column)
                # Nothing to do here
                continue

            # dict: Computed expression
            # It can only have one item: { $min: {...} } or so
            if not isinstance(comp_expression, dict):
                raise InvalidQueryError('Aggregate: Expression for "{}" should be either a column name, or an object'
                                        .format(comp_field_label))
            if len(comp_expression) != 1:
                raise InvalidQueryError('Aggregate: expression for "{}" can only contain a single aggregation operator'
                                        .format(comp_field_label))

            # Okay, the dict { $max: expression } has just one value
            agg_operator, expression = comp_expression.popitem()

            # Now we process the following data:
            # operator: '$min', '$max', etc
            # expression:
            #  1) 1: special case for $sum
            #  2) string: reference to a column. E.g. min(age)
            #  3) dict: a boolean expression. E.g. { $sum: { age: { $gt: 18 } } } }
            if isinstance(expression, int) and agg_operator == '$sum':
                # 1) special case for { $sum: 1 }
                operator_obj = self._BOOLEAN_COUNT_CLS(comp_field_label, int(expression))
            elif isinstance(expression, string_types):
                # 2) column name
                column_name = expression
                column = self._get_column_securely(column_name)
                is_column_json = column_name in self.bags.columns and self.bags.columns.is_column_json(column_name)
                operator_obj = self._COLUMN_OPERATOR_CLS(comp_field_label, agg_operator,
                                                         column_name, column, is_column_json)
            elif isinstance(expression, dict):
                # 3) Boolean expression: use MongoFilter
                bool_expression = self.mongofilter.input(expression)  #type: MongoFilter
                operator_obj = self._BOOLEAN_COUNT_CLS(comp_field_label, bool_expression)
            else:
                raise AssertionError('Aggregate: expression should be either a column name, or an object')

            # Add it
            agg_spec[comp_field_label] = operator_obj

        return agg_spec

    def compile_statements(self):
        """ Create a list of selectable statements from aggregation spec
        :rtype: list[sqlalchemy.sql.elements.ColumnElement]
        """
        return [agg_col.compile()
                for agg_col in self.agg_spec.values()]


class MongoAggregateInsecure(MongoAggregate):
    """ An insecure version of MongoAggregate

        This is a transitional class that ensures compatibility with previous versions of MongoSQL
    """
    _get_column_securely = MongoAggregate._get_column_insecurely
