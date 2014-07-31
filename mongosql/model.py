from sqlalchemy import inspect

from .statements import MongoProjection, MongoSort, MongoGroup, MongoCriteria, MongoJoin, MongoAggregate
from .bag import ModelPropertyBags


class MongoModel(object):
    """ Sqlalchemy Model wrapper that generates query chunks """

    @classmethod
    def get_for(cls, model):
        """ Get MongoModel for a model.

        Attempts to use `mongomodel` property of the model

        :param model: Model
        :type model: mongosql.MongoSqlBase|sqlalchemy.ext.declarative.DeclarativeMeta
        :rtype: MongoModel
        """
        try:
            return model.mongomodel()
        except AttributeError:
            model.mongomodel = MongoModel(model)
            return model.mongomodel

    def __init__(self, model):
        """ Create MongoSql from a model

            :type model: sqlalchemy.ext.declarative.declarative_base
            :param model: The model to build queries for
        """
        #: The model we're working with
        self.__model = model

        #: Property bags
        self.__bag = ModelPropertyBags(model)

    @property
    def model(self):
        """ Get model
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        return self.__model

    @property
    def model_bag(self):
        """ Get model columns bag
        :rtype: mongosql.bag.ModelPropertyBags
        """
        return self.__bag

    #region Wrappers

    def project(self, projection, as_relation):
        """ Build projection for a Query

        :type projection: None | dict | Iterable
        :param projection: Projection spec
        :param as_relation: Load interface to chain the loader options from
        :type as_relation: sqlalchemy.orm.Load
        :returns: Query options to load specific columns
            Usage:
                p = MongoModel(User).project(['login', 'email'])
                query.options(p)
        :rtype: sqlalchemy.orm.Load
        :raises AssertionError: invalid input
        :raises AssertionError: unknown column name
        """
        return MongoProjection(projection)(self, as_relation)

    def sort(self, sort_spec):
        """ Build sorting for a Query

            :type sort_spec: None | OrderedDict | Iterable
            :param sort_spec: Sort spec
            :returns: List of columns, with ordering modifiers applied.
                Usage:
                    s = MongoModel(User).sort(['state+'])
                    query.order_by(*s)
            :rtype: sqlalchemy.orm.query.Query
            :raises AssertionError: invalid input
            :raises AssertionError: unknown column name
        """
        return MongoSort(sort_spec)(self)

    def group(self, group_spec):
        """ Build grouping for a Query

            See :meth:sort()

            :returns: List of columns, with grouping modifiers applied.
                Usage:
                    s = MongoModel(User).group(['state'])
                    query.group_by(*s)
            :rtype: sqlalchemy.orm.query.Query
            :raises AssertionError: invalid input
            :raises AssertionError: unknown column name
        """
        return MongoGroup(group_spec)(self)

    def filter(self, criteria):
        """ Build filtering condition for a Query

            :type criteria: None | dict
            :param criteria: The criteria to filter with
            :rtype: sqlalchemy.sql.elements.BooleanClauseList
            :returns: Filtering conditions.
                Usage:
                    c = MongoModel(User).filter({ 'id': 1 })
                    query.filter(c)
            :raises AssertionError: invalid input
            :raises AssertionError: unknown column name
        """
        return MongoCriteria(criteria)(self)

    def skip(self, skip=None):
        """ Build the rows offset value

        :type skip: int | None
        :param skip: The number of rows to skip
        :returns: Offset value.
            Usage:
                skip = MongoModel(User).skip(10)
                query.offset(skip)
        :rtype: int | None
        :raises AssertionError: invalid input
        """
        assert skip is None or isinstance(skip, int), 'Skip must be one of: None, int'
        return None if skip is None or skip <= 0 else skip

    def limit(self, limit=None, skip=None):
        """ Build the rows limit & offset

            :type limit: int | None
            :param limit: Row count limitation
            :type skip: int | None
            :param skip: The number of rows to skip
            :returns: (limit, offset)
                Usage:
                    skip, limit = MongoModel(User).limit(20, 10)
                    query.limit(limit).offset(skip)
            :rtype: (int|None, int|None)
            :raises AssertionError: invalid input
        """
        assert limit is None or isinstance(limit, int), 'Limit must be one of: None, int'
        return (
            None if limit is None or limit <= 0 else limit,
            self.skip(skip)
        )

    def join(self, relnames, as_relation):
        """ Build eager loader for the relations

            :type relnames: None | Iterable[str] | dict
            :param relnames: List of relations to load eagerly
            :param as_relation: Load interface to chain the loader options from
            :type as_relation: sqlalchemy.orm.Load
            :returns: Join params list
                Usage: don't ask :)
            :returns: (list[query-options], list[(join, query-dict)])
            :rtype: list[mongosql.statements._MongoJoinParams]
            :raises AssertionError: invalid input
        """
        return MongoJoin(relnames)(self, as_relation)

    def aggregate(self, agg_spec):
        """ Select aggregated results

        :param agg_spec: Aggregation spec
        :type agg_spec: None | dict
        :return: List of selectables.
                Usage:
                    a = MongoModel(User).aggregate({ oldest: { $max: 'age' } })
                    query.add_columns(*a)
        :rtype: list[sqlalchemy.sql.elements.ColumnElement]
        :raises AssertionError: invalid input
        """
        return MongoAggregate(agg_spec)(self)

    #endregion
